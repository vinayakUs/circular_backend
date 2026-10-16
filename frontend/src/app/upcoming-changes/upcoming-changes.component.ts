import { CommonModule } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  OnInit,
  computed,
  inject,
  signal,
} from '@angular/core';
import { Router, RouterLink } from '@angular/router';
import { addDays, format } from 'date-fns';

import { CurrentPersonaService } from '../services/current-persona.service';
import { LoginService } from '../services/login.service';
import { DeadlineEvent, PersonasApiService } from '../services/personas-api.service';

interface SourceOption {
  value: string; // 'ALL' or a source token used to filter events
  label: string; // chip text
}

interface SeverityOption {
  value: string; // 'ALL' or a deadline-priority token
  label: string;
}

interface PeriodOption {
  value: '1W' | '2W' | '1M' | '2M' | '3M' | 'OTHER';
  label: string;
}

/** Period chip → day-window-size for the API. */
const PERIOD_DAYS: Record<Exclude<PeriodOption['value'], 'OTHER'>, number> = {
  '1W': 7,
  '2W': 14,
  '1M': 30,
  '2M': 60,
  '3M': 90,
};

/** Period chip → human-readable label for the summary strip. */
const PERIOD_LABELS: Record<Exclude<PeriodOption['value'], 'OTHER'>, string> = {
  '1W': 'next 1 week',
  '2W': 'next 2 weeks',
  '1M': 'next 1 month',
  '2M': 'next 2 months',
  '3M': 'next 3 months',
};

@Component({
  selector: 'app-upcoming-changes',
  standalone: true,
  imports: [CommonModule, RouterLink],
  templateUrl: './upcoming-changes.component.html',
  styleUrl: './upcoming-changes.component.css',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class UpcomingChangesComponent implements OnInit {
  /** Currently-active source chip. */
  readonly sourceFilter = signal<string>('ALL');

  /** Currently-active severity filter. */
  readonly severityFilter = signal<string>('ALL');

  /** Currently-active period filter (default 1 month). */
  readonly periodFilter = signal<PeriodOption['value']>('1M');

  /**
   * Custom date window used when the user picks the `OTHER` period chip.
   * Empty string = not yet chosen; the load logic falls back to defaults.
   * Stored as 'yyyy-MM-dd' to match <input type="date"> values.
   */
  readonly customFromDate = signal<string>('');
  readonly customToDate = signal<string>('');

  /** Whether the "More filters" panel is expanded. */
  readonly filtersExpanded = signal<boolean>(false);

  /**
   * Whether the persona chip is currently filtering deadlines to the
   * signed-in user's persona. True by default if a persona is set, false
   * when signed out / no persona. Clicking the chip toggles this.
   */
  readonly usePersonaFilter = signal<boolean>(false);

  /** Fetched deadlines (visible after each filter change). */
  readonly deadlines = signal<DeadlineEvent[]>([]);

  /** True while a fetch is in flight. */
  readonly deadlinesLoading = signal<boolean>(false);

  /** True if the last fetch errored. */
  readonly deadlinesError = signal<boolean>(false);

  /** True when OTHER is selected but the user hasn't picked both dates yet. */
  readonly dateWindowIncomplete = signal<boolean>(false);

  /** Render this list of chips in the "SOURCES" row. */
  readonly sourceOptions: ReadonlyArray<SourceOption> = [
    { value: 'ALL',  label: 'All sources' },
    { value: 'NSE',  label: 'NSE' },
    { value: 'SEBI', label: 'SEBI' },
  ];

  /** Severity choices rendered in the expanded "More filters" panel. */
  readonly severityOptions: ReadonlyArray<SeverityOption> = [
    { value: 'ALL',      label: 'All' },
    { value: 'critical', label: 'Critical' },
    { value: 'high',     label: 'High' },
    { value: 'medium',   label: 'Medium' },
    { value: 'low',      label: 'Low' },
  ];

  /** Period chip row (also rendered in the expanded "More filters" panel). */
  readonly periodOptions: ReadonlyArray<PeriodOption> = [
    { value: '1W', label: '1W' },
    { value: '2W', label: '2W' },
    { value: '1M', label: '1M' },
    { value: '2M', label: '2M' },
    { value: '3M', label: '3M' },
    { value: 'OTHER', label: 'Other' },
  ];

  private router = inject(Router);
  private auth = inject(LoginService);
  private personaSvc = inject(CurrentPersonaService);
  private api = inject(PersonasApiService);

  /** Whether the visitor is signed in. */
  readonly isAuthenticated = () => this.auth.isAuthenticated();

  /** Current persona object (null when signed out / none selected). */
  readonly currentPersona = this.personaSvc.persona;

  /** True when neither 'ALL' nor any 'NSE/SEBI/...' chip has narrowed. */
  readonly showActivePersonaChip = computed(
    () => this.isAuthenticated() && !!this.currentPersona(),
  );

  ngOnInit(): void {
    // Default to persona-scoped when a persona is set, else all.
    if (this.isAuthenticated() && this.personaSvc.slug()) {
      this.usePersonaFilter.set(true);
    }
    this.loadDeadlines();
  }

  /** Set the active source chip and refetch. */
  setSourceFilter(value: string): void {
    if (this.sourceFilter() === value) return;
    this.sourceFilter.set(value);
    this.loadDeadlines();
  }

  /** Update the active severity filter and refetch. */
  onSeverityChange(value: string): void {
    this.severityFilter.set(value);
    this.loadDeadlines();
  }

  /** Set the active period chip and refetch. */
  setPeriodFilter(value: PeriodOption['value']): void {
    if (this.periodFilter() === value) return;
    this.periodFilter.set(value);
    this.loadDeadlines();
  }

  /** Update one of the custom date inputs and refetch. */
  onCustomDateChange(which: 'from' | 'to', value: string): void {
    if (which === 'from') {
      this.customFromDate.set(value);
    } else {
      this.customToDate.set(value);
    }
    // Refetch only when the user is in the OTHER mode; otherwise the
    // period chip drives the window and ignores these inputs.
    if (this.periodFilter() === 'OTHER') {
      this.loadDeadlines();
    }
  }

  /** Toggle the "More filters" panel. */
  toggleFilters(): void {
    this.filtersExpanded.update((v) => !v);
  }

  /**
   * Persona chip click — toggles between persona-scoped and cross-persona
   * filtering, then refetches. When a persona is selected the chip is
   * filled; click again to deselect (outlined) and see all-persona data.
   */
  onPersonaChipClick(): void {
    if (!this.showActivePersonaChip()) return;
    this.usePersonaFilter.update((v) => !v);
    this.loadDeadlines();
  }

  /** True when the persona filter is currently active. */
  isPersonaFilterActive(): boolean {
    return this.usePersonaFilter() && !!this.personaSvc.slug();
  }

  /**
   * Human-readable description of the active date window, used in the
   * summary strip above the list. Returns null when the user is in
   * `OTHER` mode but hasn't picked both dates yet (no fetch fires).
   */
  windowLabel(): string | null {
    const period = this.periodFilter();
    if (period === 'OTHER') {
      const rawFrom = this.customFromDate();
      const rawTo   = this.customToDate();
      if (!rawFrom || !rawTo) return null;
      return `from ${this.formatDateLabel(rawFrom)} to ${this.formatDateLabel(rawTo)}`;
    }
    return PERIOD_LABELS[period];
  }

  /** Human-readable count line ("1 change" / "2 changes" / "no changes"). */
  countLabel(): string {
    const n = this.deadlines().length;
    if (n === 0) return 'no changes';
    if (n === 1) return '1 change';
    return `${n} changes`;
  }

  /** '2026-10-22' → '22 Oct 2026'. */
  private formatDateLabel(iso: string): string {
    // Parse manually to avoid TZ drift; date-fns format() expects a Date.
    const [y, m, d] = iso.split('-').map(Number);
    const date = new Date(y, m - 1, d);
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return `${d} ${months[m - 1]} ${y}`;
  }

  /** Clicked a deadline row — open it. */
  onDeadlineClick(event: DeadlineEvent, ev: MouseEvent): void {
    ev.preventDefault();
    if (!event.url) return;
    if (event.url.startsWith('http://') || event.url.startsWith('https://')) {
      window.open(event.url, '_blank', 'noopener,noreferrer');
      return;
    }
    this.router.navigateByUrl(event.url);
  }

  /**
   * Fetch deadlines based on the current filter state.
   *
   * Picks the persona-scoped endpoint when `usePersonaFilter` is on AND
   * the user has a persona set; otherwise uses the cross-persona
   * `/api/deadlines` endpoint. Filters translate directly into the
   * service's option names.
   *
   * In `OTHER` period mode BOTH custom from/to must be set before any
   * fetch fires — the user picks both dates explicitly, we don't
   * auto-fill a missing side with a default.
   */
  private loadDeadlines(): void {
    const today = new Date();
    const period = this.periodFilter();

    let from: Date;
    let to: Date;

    if (period === 'OTHER') {
      const rawFrom = this.customFromDate();
      const rawTo   = this.customToDate();
      if (!rawFrom || !rawTo) {
        // Missing one side — don't fire, show the prompt.
        this.deadlinesLoading.set(false);
        this.deadlinesError.set(false);
        this.deadlines.set([]);
        this.dateWindowIncomplete.set(true);
        return;
      }
      this.dateWindowIncomplete.set(false);
      from = new Date(`${rawFrom}T00:00:00`);
      to   = new Date(`${rawTo}T23:59:59`);
      // Defensive: if user picked a backwards range, don't fire a confusing
      // request — leave the previous result set and surface a clear error.
      if (to < from) {
        this.deadlinesLoading.set(false);
        this.deadlinesError.set(false);
        this.deadlines.set([]);
        return;
      }
    } else {
      this.dateWindowIncomplete.set(false);
      from = today;
      to   = addDays(today, PERIOD_DAYS[period]);
    }

    const baseOptions = {
      from: format(from, 'yyyy-MM-dd'),
      to: format(to, 'yyyy-MM-dd'),
      exchange: this.sourceFilter() === 'ALL' ? undefined : this.sourceFilter(),
      priority: this.severityFilter() === 'ALL' ? undefined : this.severityFilter(),
      limit: 100,
    };

    const slug = this.personaSvc.slug();
    const usePersona = this.usePersonaFilter() && !!slug;

    this.deadlinesLoading.set(true);
    this.deadlinesError.set(false);

    const request$ = usePersona
      ? this.api.getDeadlinesForPersona(slug!, baseOptions)
      : this.api.getDeadlines(baseOptions);

    request$.subscribe({
      next: (events) => {
        this.deadlines.set(events ?? []);
        this.deadlinesLoading.set(false);
      },
      error: () => {
        this.deadlines.set([]);
        this.deadlinesLoading.set(false);
        this.deadlinesError.set(true);
      },
    });
  }
}
