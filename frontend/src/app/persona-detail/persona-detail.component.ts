import { CommonModule } from '@angular/common';
import { Component, OnInit, computed, inject, signal } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';

import { LoginService } from '../services/login.service';
import { Persona, PersonasApiService } from '../services/personas-api.service';
import {
  DeadlineCalendarComponent,
  DeadlineEvent,
  UUID,
} from '../shared/deadline-calendar/deadline-calendar.component';

// ─────────────────────────────────────────────────────────────────────
// HARDCODED CALENDAR DATA — REMOVE WHEN BACKEND ENDPOINT IS READY
// Replace `calendarEvents` and `filterDeadline()` with a real API call to
// `GET /api/personas/:slug/deadlines` (or similar). The mock below gives
// the calendar widget something to display while the backend is pending.
// ─────────────────────────────────────────────────────────────────────
function makeMockDeadlines(slug: string | null): DeadlineEvent[] {
  if (!slug) return [];

  // Anchor on the start of the current month so days render predictably.
  const today = new Date();
  const year = today.getFullYear();
  const month = today.getMonth();

  const day = (d: number) => new Date(year, month, d);
  // Cast literal strings to UUID (backend will produce real UUIDs).
  const id = (s: string) => s as UUID;

  // Different mock data per persona — shows realistic content per role.
  if (slug === 'compliance-officer') {
    return [
      { id: id('11111111-1111-1111-1111-111111111111'), date: day(5),  title: 'KYC attribute submission deadline for new members',     exchange: 'NSE',  priority: 'critical', url: '/circulars/aaa' },
      { id: id('22222222-2222-2222-2222-222222222222'), date: day(5),  title: 'AML reporting window opens for Q3',                exchange: 'SEBI', priority: 'high',     url: '/circulars/bbb' },
      { id: id('33333333-3333-3333-3333-333333333333'), date: day(8),  title: 'Penalty review — non-compliant trading members', exchange: 'BSE',  priority: 'medium',   url: '/circulars/ccc' },
      { id: id('44444444-4444-4444-4444-444444444444'), date: day(13), title: 'Quarterly compliance certificate due',              exchange: 'NSE',  priority: 'high',     url: '/circulars/ddd' },
      { id: id('55555555-5555-5555-5555-555555555555'), date: day(20), title: 'Annual AML audit report deadline',                 exchange: 'SEBI', priority: 'medium',   url: '/circulars/eee' },
    ];
  }

  if (slug === 'trading-desk') {
    return [
      { id: id('aaaaaaaa-1111-1111-1111-aaaaaaaaaaaa'), date: day(3),  title: 'Auction settlement — G-Sec (SDL)',                   exchange: 'BSE', priority: 'critical', url: '/circulars/ggg' },
      { id: id('aaaaaaaa-2222-2222-2222-aaaaaaaaaaaa'), date: day(10), title: 'Mock SIP/PMS registration window',                   exchange: 'NSE', priority: 'high',     url: '/circulars/hhh' },
      { id: id('aaaaaaaa-3333-3333-3333-aaaaaaaaaaaa'), date: day(13), title: 'Options expiry — weekly contract adjustments',     exchange: 'NSE', priority: 'critical', url: '/circulars/iii' },
      { id: id('aaaaaaaa-4444-4444-4444-aaaaaaaaaaaa'), date: day(18), title: 'Index rebalance effective',                        exchange: 'NSE', priority: 'medium',   url: '/circulars/jjj' },
    ];
  }

  if (slug === 'risk-manager') {
    return [
      { id: id('bbbbbbbb-1111-1111-1111-bbbbbbbbbbbb'), date: day(7),  title: 'VaR margin recalibration',                          exchange: 'NSE',  priority: 'critical', url: '/circulars/kkk' },
      { id: id('bbbbbbbb-2222-2222-2222-bbbbbbbbbbbb'), date: day(13), title: 'Initial margin rate revision effective',           exchange: 'BSE',  priority: 'high',     url: '/circulars/lll' },
      { id: id('bbbbbbbb-3333-3333-3333-bbbbbbbbbbbb'), date: day(22), title: 'Risk-weight review for new instruments',           exchange: 'SEBI', priority: 'medium',   url: '/circulars/mmm' },
    ];
  }

  // Generic fallback for any other slug
  return [
    { id: id('cccccccc-1111-1111-1111-cccccccccccc'), date: day(13), title: 'Mock deadline for this persona',         exchange: 'NSE',  priority: 'high',     url: '/circulars/xxx' },
    { id: id('cccccccc-2222-2222-2222-cccccccccccc'), date: day(18), title: 'Mock review window opens',               exchange: 'SEBI', priority: 'medium',   url: '/circulars/yyy' },
    { id: id('cccccccc-3333-3333-3333-cccccccccccc'), date: day(25), title: 'Mock compliance filing deadline',         exchange: 'BSE',  priority: 'low',      url: '/circulars/zzz' },
  ];
}
// ───────────────────────────────────────────────────────────────────────

@Component({
  selector: 'app-persona-detail',
  standalone: true,
  imports: [CommonModule, RouterLink, DeadlineCalendarComponent],
  templateUrl: './persona-detail.component.html',
  styleUrl: './persona-detail.component.css',
})
export class PersonaDetailComponent implements OnInit {
  persona: Persona | null = null;
  isAuthenticated = false;
  loading = true;
  error: string | null = null;

  /** Hardcoded — remove when backend deadline endpoint exists. */
  calendarEvents: DeadlineEvent[] = [];

  /** Active source filter for the calendar. */
  sourceFilter = signal<string>('ALL');

  /** Filter chips rendered above the calendar. */
  readonly sourceFilterOptions: ReadonlyArray<{ value: string; label: string }> = [
    { value: 'ALL',  label: 'All sources' },
    { value: 'SEBI', label: 'SEBI' },
    { value: 'NSE',  label: 'NSE'  },
  ];

  /** Events shown in the calendar after source filter is applied. */
  filteredCalendarEvents = computed(() => {
    const filter = this.sourceFilter();
    if (filter === 'ALL') return this.calendarEvents;
    return this.calendarEvents.filter((e) => e.exchange === filter);
  });

  private route = inject(ActivatedRoute);
  private personasApi = inject(PersonasApiService);
  private auth = inject(LoginService);

  ngOnInit(): void {
    this.isAuthenticated = this.auth.isAuthenticated();
    const slug = this.route.snapshot.paramMap.get('slug');

    if (!slug) {
      this.error = 'No persona specified';
      this.loading = false;
      return;
    }

    this.personasApi.getPersonas().subscribe({
      next: (all) => {
        const match = all.find((p) => p.slug === slug);
        if (!match) {
          this.error = `Unknown persona: ${slug}`;
        } else {
          this.persona = match;
          // TODO: replace with real fetch from /api/personas/:slug/deadlines
          this.calendarEvents = makeMockDeadlines(slug);
        }
        this.loading = false;
      },
      error: () => {
        this.error = 'Could not load personas';
        this.loading = false;
      },
    });
  }

  /** Set the active source filter. */
  setSourceFilter(value: string): void {
    this.sourceFilter.set(value);
  }

  /** Count events per source for chip labels. */
  countBySource(value: string): number {
    if (value === 'ALL') return this.calendarEvents.length;
    return this.calendarEvents.filter((e) => e.exchange === value).length;
  }
}