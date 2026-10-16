import { CommonModule } from '@angular/common';
import { Component, OnInit, computed, inject, signal } from '@angular/core';
import { ActivatedRoute, Router, RouterLink } from '@angular/router';

import { LoginService } from '../services/login.service';
import {
  DeadlineEvent,
  Persona,
  PersonasApiService,
} from '../services/personas-api.service';
import { DeadlineCalendarComponent } from '../shared/deadline-calendar/deadline-calendar.component';
import { UpcomingDeadlinesComponent } from '../shared/upcoming-deadlines/upcoming-deadlines.component';
import { addDays, endOfMonth, format } from 'date-fns';

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
  isAuthenticated = false;
  loading = true;
  error: string | null = null;

  /** Deadlines for the month currently shown in the calendar. */
  readonly calendarEvents = signal<DeadlineEvent[]>([]);

  /** Month-keyed cache — key is 'MM-yyyy', exactly what the calendar emits. */
  private readonly deadlineCache = new Map<string, DeadlineEvent[]>();

  /** Active source filter for the calendar. */
  sourceFilter = signal<string>('ALL');

  /** Non-null while a month fetch is in flight; disables calendar navigation. */
  readonly loadingMonth = signal<string | null>(null);

  /** Upcomming deadlines for that particular widget */
  readonly upcomingEvents = signal<DeadlineEvent[]>([]);
  readonly loadingUpcoming = signal<boolean>(false);

  /** Filter chips rendered above the calendar. */
  readonly sourceFilterOptions: ReadonlyArray<{
    value: string;
    label: string;
  }> = [
    { value: 'ALL', label: 'All sources' },
    { value: 'SEBI', label: 'SEBI' },
    { value: 'NSE', label: 'NSE' },
  ];

  /** Events shown in the calendar after source filter is applied. */
  filteredCalendarEvents = computed(() => {
    const events = this.calendarEvents();
    const filter = this.sourceFilter();
    if (filter === 'ALL') return events;
    return events.filter((e) => e.exchange === filter);
  });

  private route = inject(ActivatedRoute);
  private personasApi = inject(PersonasApiService);
  private auth = inject(LoginService);
  private router = inject(Router);

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
          this.loadDeadlines(slug, format(new Date(), 'MM-yyyy'));
          // in ngOnInit success branch, next to loadDeadlines():
          this.loadUpcomingDeadlines(slug); // from=today, to=today+60, limit=5
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

    console.log(source);
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

  /** Calendar emitted a month change — fetch that month, or serve it from cache. */
  onMonthChange(monthKey: string): void {
    if (this.persona) {
      this.loadDeadlines(this.persona.slug, monthKey);
    }
  }

  /** Load one month of deadlines; cache hit = no HTTP call. */
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

  /** Clicked an upcoming-deadline row — open it if it has a URL. */
  onUpcomingEventClicked(event: DeadlineEvent): void {
    if (!event.url) return;
    if (event.url.startsWith('http://') || event.url.startsWith('https://')) {
      window.open(event.url, '_blank', 'noopener');
      return;
    }

    this.router.navigateByUrl(event.url);
  }

  /** Calendar emitted `viewAll` — jump to the cross-persona Changes page. */
  onViewAll(): void {
    this.router.navigate(['/upcoming-changes']);
  }

  /** Set the active source filter. */
  setSourceFilter(value: string): void {
    if (this.sourceFilter() === value) return;
    this.sourceFilter.set(value);
    if (this.persona) this.loadUpcomingDeadlines(this.persona.slug);
  }

  /** Count events per source for chip labels. */
  countBySource(value: string): number {
    const events = this.calendarEvents();
    if (value === 'ALL') return events.length;
    return events.filter((e) => e.exchange === value).length;
  }

  /** 'MM-yyyy' → first/last day of that month as 'yyyy-MM-dd'. */
  private monthWindow(monthKey: string): { from: string; to: string } {
    const [mm, yyyy] = monthKey.split('-').map(Number);
    const first = new Date(yyyy, mm - 1, 1);
    return {
      from: format(first, 'yyyy-MM-dd'),
      to: format(endOfMonth(first), 'yyyy-MM-dd'),
    };
  }
}
