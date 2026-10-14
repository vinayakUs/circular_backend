"""Unit tests for the circular classifier (exact-match, NSE-only).

Match rule:
    circular.source == filter.source (case-insensitive)
    circular.department EQUALS one of filter.departments (case-insensitive)
"""
import unittest

from app.services.circular_classifier import CircularRef, classify, matches
from app.services.personas import Persona, PersonaFilter


def make_persona(slug, filters):
    return Persona(
        slug=slug,
        name=slug.title(),
        description=f"Test persona {slug}",
        icon="test-icon",
        filters=filters,
        keywords=[],
    )


def make_filter(source, departments=None, categories=None):
    return PersonaFilter(
        source=source,
        departments=departments or [],
        categories=categories or [],
    )


# ─────────────────────────────────────────────────────────────────────
# Source matching
# ─────────────────────────────────────────────────────────────────────
class SourceMatchingTestCase(unittest.TestCase):

    def test_nse_source_matches(self):
        ref = CircularRef(source="NSE", department="Surveillance & Investigation")
        persona = make_persona("trading-desk", [
            make_filter(source="NSE", departments=["Surveillance & Investigation"]),
        ])
        self.assertTrue(matches(ref, persona))

    def test_sebi_source_does_not_match_nse_filter(self):
        """NSE-only classifier — SEBI circulars don't match NSE filters."""
        ref = CircularRef(source="SEBI", department="MIRSD")
        persona = make_persona("trading-desk", [
            make_filter(source="NSE", departments=["MIRSD"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_source_case_insensitive(self):
        ref = CircularRef(source="nse", department="Surveillance & Investigation")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["Surveillance & Investigation"]),
        ])
        self.assertTrue(matches(ref, persona))

    def test_source_with_whitespace_stripped(self):
        ref = CircularRef(source="  NSE  ", department="Surveillance & Investigation")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["Surveillance & Investigation"]),
        ])
        self.assertTrue(matches(ref, persona))

    def test_empty_source_does_not_match(self):
        ref = CircularRef(source="", department="Surveillance & Investigation")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["Surveillance & Investigation"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_none_department_does_not_match_specific_filter(self):
        """circular.department is None — can't equal any string."""
        ref = CircularRef(source="NSE", department=None)
        persona = make_persona("narrow", [
            make_filter(source="NSE", departments=["Surveillance"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_none_department_with_empty_filter(self):
        """No dept filter = catch-all, even when circular.department is None."""
        ref = CircularRef(source="NSE", department=None)
        persona = make_persona("broad", [
            make_filter(source="NSE", departments=[]),
        ])
        self.assertTrue(matches(ref, persona))


# ─────────────────────────────────────────────────────────────────────
# Department matching — EXACT (case-insensitive, no substring)
# ─────────────────────────────────────────────────────────────────────
class DepartmentMatchingTestCase(unittest.TestCase):

    def test_exact_match(self):
        ref = CircularRef(source="NSE", department="Surveillance & Investigation")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["Surveillance & Investigation"]),
        ])
        self.assertTrue(matches(ref, persona))

    def test_exact_match_case_insensitive(self):
        ref = CircularRef(source="NSE", department="SURVEILLANCE & INVESTIGATION")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["surveillance & investigation"]),
        ])
        self.assertTrue(matches(ref, persona))

    def test_substring_does_NOT_match(self):
        """'Surveillance' alone doesn't match filter 'Surveillance & Investigation'."""
        ref = CircularRef(source="NSE", department="Surveillance")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["Surveillance & Investigation"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_superset_does_NOT_match(self):
        """'Surveillance & Investigation' doesn't match filter 'Surveillance'."""
        ref = CircularRef(source="NSE", department="Surveillance & Investigation")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["Surveillance"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_different_departments_do_not_match(self):
        ref = CircularRef(source="NSE", department="Listing")
        persona = make_persona("trading-desk", [
            make_filter(source="NSE", departments=["Surveillance & Investigation"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_plural_vs_singular_does_NOT_match(self):
        """'Receipt' (singular) doesn't match 'Receipts' (plural filter)."""
        ref = CircularRef(source="NSE", department="Electronic Gold Receipt")
        persona = make_persona("trading-desk", [
            make_filter(source="NSE", departments=["Electronic Gold Receipts"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_abbreviation_does_NOT_match_full_name(self):
        """'MIRSD' (filter) doesn't match 'Market Intermediaries Regulation...' (dept)."""
        ref = CircularRef(
            source="SEBI",
            department="Market Intermediaries Regulation and Supervision Department",
        )
        persona = make_persona("compliance-officer", [
            make_filter(source="SEBI", departments=["MIRSD"]),
        ])
        self.assertFalse(matches(ref, persona))

    def test_empty_departments_matches_any(self):
        """Catch-all — empty filter.departments list = match any department."""
        ref = CircularRef(source="NSE", department="AnythingGoes")
        persona = make_persona("broad", [
            make_filter(source="NSE", departments=[]),
        ])
        self.assertTrue(matches(ref, persona))

    def test_whitespace_in_dept_does_not_match(self):
        """Whitespace stripped — '  X  ' matches 'X'."""
        ref = CircularRef(source="NSE", department="  Surveillance  ")
        persona = make_persona("risk-manager", [
            make_filter(source="NSE", departments=["Surveillance"]),
        ])
        self.assertTrue(matches(ref, persona))

    def test_empty_department_string_with_specific_filter(self):
        """Empty string is not a substring of any non-empty filter word — no match."""
        ref = CircularRef(source="NSE", department="")
        persona = make_persona("narrow", [
            make_filter(source="NSE", departments=["Surveillance"]),
        ])
        self.assertFalse(matches(ref, persona))


# ─────────────────────────────────────────────────────────────────────
# Multi-filter group semantics
# ─────────────────────────────────────────────────────────────────────
class MultiFilterTestCase(unittest.TestCase):

    def test_or_semantics_one_group_matches(self):
        ref = CircularRef(source="NSE", department="Listing")
        persona = make_persona("multi", [
            make_filter(source="NSE", departments=["Surveillance"]),  # fails
            make_filter(source="NSE", departments=["Listing", "Mutual Fund"]),  # matches
        ])
        self.assertTrue(matches(ref, persona))

    def test_all_groups_fail(self):
        ref = CircularRef(source="NSE", department="SomeRandomDept")
        persona = make_persona("multi", [
            make_filter(source="NSE", departments=["Surveillance"]),
            make_filter(source="NSE", departments=["Listing"]),
        ])
        self.assertFalse(matches(ref, persona))


# ─────────────────────────────────────────────────────────────────────
# Real config — end-to-end
# ─────────────────────────────────────────────────────────────────────
class RealConfigTestCase(unittest.TestCase):

    def test_nse_exact_match_real_config(self):
        """Department must match a filter item EXACTLY."""
        from app.services.personas import get_personas
        personas = get_personas()

        # 'Surveillance & Investigation' is in compliance-officer NSE list
        ref = CircularRef(source="NSE", department="Surveillance & Investigation")
        result = classify(ref, personas)
        self.assertIn("compliance-officer", result)
        self.assertIn("risk-manager", result)
        self.assertIn("fpi", result)
        self.assertNotIn("investor", result)

    def test_sebi_full_names_match_real_departments(self):
        """Config now uses full names (e.g. 'Market Intermediaries Regulation...'),
        so the matching dept name should appear in the matched personas."""
        from app.services.personas import get_personas
        personas = get_personas()
        ref = CircularRef(
            source="SEBI",
            department="Market Intermediaries Regulation and Supervision Department",
        )
        result = classify(ref, personas)
        # Config lists this full name under compliance, trading, risk, operations
        self.assertIn("compliance-officer", result)
        self.assertIn("trading-desk", result)
        self.assertIn("risk-manager", result)
        self.assertIn("operations", result)

    def test_sebi_circular_returns_no_matches(self):
        """SEBI matching is disabled by source check (config has SEBI, circulars have SEBI_MASTER)."""
        from app.services.personas import get_personas
        personas = get_personas()
        ref = CircularRef(source="SEBI_MASTER", department="MIRSD")
        result = classify(ref, personas)
        self.assertEqual(result, [])

    def test_circularref_is_frozen(self):
        ref = CircularRef(source="NSE", department="X")
        with self.assertRaises(Exception):
            ref.source = "SEBI"


if __name__ == "__main__":
    unittest.main()