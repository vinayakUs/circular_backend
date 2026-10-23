import { CommonModule } from '@angular/common';
import { Component, OnInit, computed, inject, signal } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';

import { AuthService } from '../services/auth.service';
import {
  DeadlineEvent,
  Persona,
  PersonasApiService,
} from '../services/personas-api.service';
import { DeadlineCalendarComponent } from '../shared/deadline-calendar/deadline-calendar.component';
import { UpcomingDeadlinesComponent } from '../shared/upcoming-deadlines/upcoming-deadlines.component';
import { addDays, endOfMonth, format, subDays } from 'date-fns';

import { Circular } from '../services/circulars-api.service';
import { PersonaDetailFiltersService } from './persona-detail-filters.service';
import { UpcomingPeriod } from '../upcoming-changes/upcoming-changes-filters.service';
import { normalizeSource } from '../util/source.util';

/** Period chip -> how many days back from today (over `issue_date`). */
const PERIOD_DAYS: Record<Exclude<UpcomingPeriod, 'OTHER'>, number> = {
  '1W': 7,
  '2W': 14,
  '1M': 30,
  '2M': 60,
  '3M': 90,
};

/** Human wording for the active window, shown in the summary strip. */
const PERIOD_LABELS: Record<Exclude<UpcomingPeriod, 'OTHER'>, string> = {
  '1W': 'last 1 week',
  '2W': 'last 2 weeks',
  '1M': 'last 1 month',
  '2M': 'last 2 months',
  '3M': 'last 3 months',
};

/** Page size for the circulars list. Matches the All Circulars page. */
const CIRCULARS_PAGE_SIZE = 10;

@Component({
  selector: 'app-persona-detail',
  standalone: true,
  imports: [
    CommonModule,
    RouterLink,
    DeadlineCalendarComponent,
    UpcomingDeadlinesComponent,
  ],
  templateUrl: './persona-detail.component.html',
  styleUrl: './persona-detail.component.css',
})
export class PersonaDetailComponent implements OnInit {
  persona: Persona | null = null;
  loading = true;
  error: string | null = null;

  readonly calendarEvents = signal<DeadlineEvent[]>([]);

  // Key is 'MM-yyyy', exactly what the calendar emits.
  private readonly deadlineCache = new Map<string, DeadlineEvent[]>();

  sourceFilter = signal<string>('ALL');

  // Non-null while a month fetch is in flight; disables calendar navigation.
  readonly loadingMonth = signal<string | null>(null);

  readonly upcomingEvents = signal<DeadlineEvent[]>([]);
  readonly loadingUpcoming = signal<boolean>(false);

  // ── Applicable circulars list ─────────────────────────────────
  // Period state lives in a root service so it survives navigation to the
  // circular detail page and back.
  private readonly filters = inject(PersonaDetailFiltersService);

  readonly periodFilter    = this.filters.periodFilter;
  readonly customFromDate  = this.filters.customFromDate;
  readonly customToDate    = this.filters.customToDate;

  readonly circulars            = signal<Circular[]>([]);
  readonly circularsTotal       = signal<number>(0);
  readonly circularsLoading     = signal<boolean>(false);
  readonly circularsError       = signal<boolean>(false);
  readonly dateWindowIncomplete = signal<boolean>(false);
  readonly circularOffset       = signal<number>(0);

  /**
   * True when the list body has cards to render.
   *
   * Deliberately NOT gated on `circularsLoading()`. The list keeps the
   * previous page mounted while a request is in flight, so gating on loading
   * would tear ten cards out of the DOM and rebuild them on every filter
   * change — the list is taller than half a viewport, so that reads as the
   * whole screen flickering. Stale results dim instead (`.is-loading`), which
   * is what the All Circulars page does.
   *
   * False for an incomplete custom range or a failed fetch, which is what
   * suppresses the summary strip so it never claims "no circulars" alongside
   * an error message.
   */
  readonly listVisible = computed(
    () =>
      !this.dateWindowIncomplete() &&
      !this.circularsError() &&
      this.circulars().length > 0,
  );

  readonly periodOptions: ReadonlyArray<{
    value: UpcomingPeriod;
    label: string;
  }> = [
    { value: '1W', label: '1W' },
    { value: '2W', label: '2W' },
    { value: '1M', label: '1M' },
    { value: '2M', label: '2M' },
    { value: '3M', label: '3M' },
    { value: 'OTHER', label: 'Other' },
  ];

  readonly sourceFilterOptions: ReadonlyArray<{
    value: string;
    label: string;
  }> = [
    { value: 'ALL', label: 'All sources' },
    { value: 'SEBI', label: 'SEBI' },
    { value: 'NSE', label: 'NSE' },
  ];

  filteredCalendarEvents = computed(() => {
    const events = this.calendarEvents();
    const filter = this.sourceFilter();
    if (filter === 'ALL') return events;
    return events.filter((e) => e.exchange === filter);
  });

  private route = inject(ActivatedRoute);
  private personasApi = inject(PersonasApiService);
  private auth = inject(AuthService);
  private router = inject(Router);

  get isAuthenticated(): boolean {
    return this.auth.isAuthenticated();
  }

  ngOnInit(): void {
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
          // Retained filters belong to whichever persona set them. Moving to
          // a different persona starts clean; a round-trip through the
          // circular detail page keeps the user's selections.
          if (this.filters.activeSlug() !== slug) {
            this.filters.setActiveSlug(slug);
            this.filters.resetToDefaults();
            this.circularOffset.set(0);
          }
          this.loadDeadlines(slug, format(new Date(), 'MM-yyyy'));
          this.loadUpcomingDeadlines(slug);
          this.loadCirculars();
        }
        this.loading = false;
      },
      error: () => {
        this.error = 'Could not load personas';
        this.loading = false;
      },
    });
  }
  loadUpcomingDeadlines(slug: string) {
    const source = this.sourceFilter();
    const today = new Date();

    this.loadingUpcoming.set(true);
    this.personasApi
      .getDeadlinesForPersona(slug, {
        from: format(today, 'yyyy-MM-dd'),
        to: format(addDays(today, 60), 'yyyy-MM-dd'),
        limit: 5,
        exchange: source === 'ALL' ? undefined : source,
      })
      .subscribe({
        next: (events) => {
          this.upcomingEvents.set(events);
          this.loadingUpcoming.set(false);
        },
        error: () => {
          this.upcomingEvents.set([]);
          this.loadingUpcoming.set(false);
        },
      });
  }

  /** Calendar emitted a month change — serve from cache if available, else fetch. */
  onMonthChange(monthKey: string): void {
    if (this.persona) {
      this.loadDeadlines(this.persona.slug, monthKey);
    }
  }

  private loadDeadlines(slug: string, monthKey: string): void {
    const cached = this.deadlineCache.get(monthKey);

    if (cached) {
      this.calendarEvents.set(cached);
      return;
    }

    this.loadingMonth.set(monthKey);
    const { from, to } = this.monthWindow(monthKey);

    this.personasApi.getDeadlinesForPersona(slug, { from, to }).subscribe({
      next: (events) => {
        this.deadlineCache.set(monthKey, events);
        this.calendarEvents.set(events);
        this.loadingMonth.set(null);
      },
      error: () => {
        // Failures are not cached, so visiting this month again retries.
        this.calendarEvents.set([]);
        this.loadingMonth.set(null);
      },
    });
  }

  onUpcomingEventClicked(event: DeadlineEvent): void {
    if (!event.url) return;
    if (event.url.startsWith('http://') || event.url.startsWith('https://')) {
      window.open(event.url, '_blank', 'noopener');
      return;
    }

    this.router.navigateByUrl(event.url);
  }

  onViewAll(): void {
    this.router.navigate(['/upcoming-changes']);
  }

  setSourceFilter(value: string): void {
    if (this.sourceFilter() === value) return;
    this.sourceFilter.set(value);
    if (!this.persona) return;
    this.circularOffset.set(0);
    this.loadUpcomingDeadlines(this.persona.slug);
    this.loadCirculars();
  }

  // ── Circulars list: filters ───────────────────────────────────
  setPeriodFilter(value: UpcomingPeriod): void {
    if (this.periodFilter() === value) return;
    this.filters.setPeriod(value);
    this.circularOffset.set(0);
    this.loadCirculars();
  }

  onCustomDateChange(which: 'from' | 'to', value: string): void {
    this.filters.setCustomDate(which, value);
    // The period chip owns the window unless the user is in OTHER mode, so
    // only refetch there.
    if (this.periodFilter() === 'OTHER') {
      this.circularOffset.set(0);
      this.loadCirculars();
    }
  }

  // ── Circulars list: pagination ────────────────────────────────
  get canGoPrev(): boolean {
    return this.circularOffset() > 0;
  }

  get canGoNext(): boolean {
    return this.circularOffset() + CIRCULARS_PAGE_SIZE < this.circularsTotal();
  }

  onPrevPage(): void {
    if (!this.canGoPrev) return;
    this.circularOffset.set(
      Math.max(0, this.circularOffset() - CIRCULARS_PAGE_SIZE),
    );
    this.loadCirculars();
  }

  onNextPage(): void {
    if (!this.canGoNext) return;
    this.circularOffset.update((o) => o + CIRCULARS_PAGE_SIZE);
    this.loadCirculars();
  }

  /** "Page 2 of 5" — null when everything fits on a single page. */
  pageLabel(): string | null {
    const total = this.circularsTotal();
    if (total <= CIRCULARS_PAGE_SIZE) return null;
    const page = Math.floor(this.circularOffset() / CIRCULARS_PAGE_SIZE) + 1;
    return `Page ${page} of ${Math.ceil(total / CIRCULARS_PAGE_SIZE)}`;
  }

  /**
   * Description of the active window for the summary strip. Null in OTHER
   * mode until both dates are picked, because no fetch fires before then.
   */
  windowLabel(): string | null {
    const period = this.periodFilter();
    if (period === 'OTHER') {
      const rawFrom = this.customFromDate();
      const rawTo   = this.customToDate();
      if (!rawFrom || !rawTo) return null;
      return `${this.formatDateLabel(rawFrom)} to ${this.formatDateLabel(rawTo)}`;
    }
    return PERIOD_LABELS[period];
  }

  countLabel(): string {
    const n = this.circularsTotal();
    if (n === 0) return 'no circulars';
    if (n === 1) return '1 circular';
    return `${n} circulars`;
  }

  // '2026-10-22' -> '22 Oct 2026'.
  private formatDateLabel(iso: string): string {
    const [y, m, d] = iso.split('-').map(Number);
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return `${d} ${months[m - 1]} ${y}`;
  }

  /**
   * Keep card DOM nodes alive across loads so the list swaps content in place
   * instead of tearing every row down and rebuilding it.
   */
  trackCircularById(_index: number, c: Circular): string {
    return c.id;
  }

  /**
   * Fetch the persona's circulars for the active window + source.
   *
   * The window is BACKWARD-looking — "last N days" over `circulars.issue_date`
   * — unlike the upcoming-changes page, whose window runs forward because it
   * filters future deadlines.
   *
   * In OTHER mode BOTH custom dates must be set before any fetch fires; we
   * don't auto-fill a missing side. A backwards range is ignored rather than
   * sent as a confusing request.
   */
  private loadCirculars(): void {
    if (!this.persona) return;

    const today = new Date();
    const period = this.periodFilter();

    let from: Date;
    let to: Date;

    if (period === 'OTHER') {
      const rawFrom = this.customFromDate();
      const rawTo   = this.customToDate();
      if (!rawFrom || !rawTo) {
        this.circularsLoading.set(false);
        this.circularsError.set(false);
        this.circulars.set([]);
        this.circularsTotal.set(0);
        this.dateWindowIncomplete.set(true);
        return;
      }
      this.dateWindowIncomplete.set(false);
      from = new Date(`${rawFrom}T00:00:00`);
      to   = new Date(`${rawTo}T23:59:59`);
      // Defensive: a backwards range would return a confusing empty page, so
      // don't fire at all.
      if (to < from) {
        this.circularsLoading.set(false);
        this.circularsError.set(false);
        this.circulars.set([]);
        this.circularsTotal.set(0);
        return;
      }
    } else {
      this.dateWindowIncomplete.set(false);
      to   = today;
      from = subDays(today, PERIOD_DAYS[period]);
    }

    this.circularsLoading.set(true);
    this.circularsError.set(false);

    this.personasApi
      .getCircularsForPersona(this.persona.slug, {
        from: format(from, 'yyyy-MM-dd'),
        to: format(to, 'yyyy-MM-dd'),
        source: this.sourceFilter(),
        limit: CIRCULARS_PAGE_SIZE,
        offset: this.circularOffset(),
      })
      .subscribe({
        next: (res) => {
          this.circulars.set(
            (res.data?.circulars ?? []).map((c) => ({
              ...c,
              normalizedSource: normalizeSource(c.source),
            })),
          );
          this.circularsTotal.set(res.pagination?.total ?? 0);
          this.circularsLoading.set(false);
        },
        error: () => {
          this.circulars.set([]);
          this.circularsTotal.set(0);
          this.circularsLoading.set(false);
          this.circularsError.set(true);
        },
      });
  }

  countBySource(value: string): number {
    const events = this.calendarEvents();
    if (value === 'ALL') return events.length;
    return events.filter((e) => e.exchange === value).length;
  }

  // 'MM-yyyy' → first/last day of that month as 'yyyy-MM-dd'.
  private monthWindow(monthKey: string): { from: string; to: string } {
    const [mm, yyyy] = monthKey.split('-').map(Number);
    const first = new Date(yyyy, mm - 1, 1);
    return {
      from: format(first, 'yyyy-MM-dd'),
      to: format(endOfMonth(first), 'yyyy-MM-dd'),
    };
  }
}
