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

  readonly calendarEvents = signal<DeadlineEvent[]>([]);

  // Key is 'MM-yyyy', exactly what the calendar emits.
  private readonly deadlineCache = new Map<string, DeadlineEvent[]>();

  sourceFilter = signal<string>('ALL');

  // Non-null while a month fetch is in flight; disables calendar navigation.
  readonly loadingMonth = signal<string | null>(null);

  readonly upcomingEvents = signal<DeadlineEvent[]>([]);
  readonly loadingUpcoming = signal<boolean>(false);

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
          this.loadUpcomingDeadlines(slug);
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
    if (this.persona) this.loadUpcomingDeadlines(this.persona.slug);
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
