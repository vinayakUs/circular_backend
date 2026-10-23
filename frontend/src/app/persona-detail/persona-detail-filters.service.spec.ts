import { TestBed } from '@angular/core/testing';

import { PersonaDetailFiltersService } from './persona-detail-filters.service';

describe('PersonaDetailFiltersService', () => {
  // The service is a root singleton and every spec here asserts post-reset
  // state, so each spec gets its own instance. The class has no injected
  // dependencies, so constructing it directly is equivalent to what TestBed
  // hands out — and it avoids leaking state between specs.
  let service: PersonaDetailFiltersService;

  beforeEach(() => {
    service = new PersonaDetailFiltersService();
  });

  it('is provided in root as a singleton', () => {
    TestBed.configureTestingModule({});
    const first = TestBed.inject(PersonaDetailFiltersService);
    const second = TestBed.inject(PersonaDetailFiltersService);
    expect(first).toBe(second);
  });

  it('defaults to a 1M period with no custom dates and no active slug', () => {
    expect(service.periodFilter()).toBe('1M');
    expect(service.customFromDate()).toBe('');
    expect(service.customToDate()).toBe('');
    expect(service.activeSlug()).toBeNull();
  });

  it('sets the period filter', () => {
    service.setPeriod('2W');
    expect(service.periodFilter()).toBe('2W');
  });

  it('accepts OTHER as a period', () => {
    service.setPeriod('OTHER');
    expect(service.periodFilter()).toBe('OTHER');
  });

  it('is a no-op when the period is unchanged', () => {
    service.setPeriod('3M');
    service.setPeriod('3M');
    expect(service.periodFilter()).toBe('3M');
  });

  it('keeps the from and to custom dates independent', () => {
    service.setCustomDate('from', '2026-01-01');
    expect(service.customFromDate()).toBe('2026-01-01');
    expect(service.customToDate()).toBe('');

    service.setCustomDate('to', '2026-01-31');
    expect(service.customFromDate()).toBe('2026-01-01');
    expect(service.customToDate()).toBe('2026-01-31');
  });

  it('is a no-op when a custom date is unchanged', () => {
    service.setCustomDate('from', '2026-01-01');
    service.setCustomDate('from', '2026-01-01');
    expect(service.customFromDate()).toBe('2026-01-01');
  });

  it('clears a custom date when set to an empty string', () => {
    service.setCustomDate('from', '2026-01-01');
    service.setCustomDate('from', '');
    expect(service.customFromDate()).toBe('');
  });

  it('tracks the active slug', () => {
    service.setActiveSlug('compliance-officer');
    expect(service.activeSlug()).toBe('compliance-officer');

    service.setActiveSlug(null);
    expect(service.activeSlug()).toBeNull();
  });

  it('resetToDefaults restores the period and clears the dates', () => {
    service.setPeriod('OTHER');
    service.setCustomDate('from', '2026-01-01');
    service.setCustomDate('to', '2026-01-31');

    service.resetToDefaults();

    expect(service.periodFilter()).toBe('1M');
    expect(service.customFromDate()).toBe('');
    expect(service.customToDate()).toBe('');
  });

  it('resetToDefaults keeps the active slug', () => {
    service.setActiveSlug('compliance-officer');
    service.setPeriod('2W');

    service.resetToDefaults();

    expect(service.activeSlug()).toBe('compliance-officer');
  });
});
