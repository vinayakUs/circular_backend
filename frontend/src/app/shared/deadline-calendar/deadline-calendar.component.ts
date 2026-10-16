import { CommonModule } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  Output,
  computed,
  signal,
} from '@angular/core';
import {
  addMonths,
  eachDayOfInterval,
  endOfMonth,
  endOfWeek,
  format,
  isSameDay,
  isSameMonth,
  isToday,
  startOfMonth,
  startOfWeek,
  subMonths,
} from 'date-fns';

/** Branded UUID type — compile-time safety for IDs coming from the backend. */
declare const __uuid: unique symbol;
export type UUID = string & { readonly [__uuid]: never };

export type DeadlinePriority = 'critical' | 'high' | 'medium' | 'low';

export interface DeadlineEvent {
  /** Backend-generated UUID. */
  id: UUID;
  /** Day this event occurs on. ISO string or Date — both accepted. */
  date: Date | string;
  /** Display text in the item list below. */
  title: string;
  /** Exchange badge (e.g. 'NSE', 'SEBI'). Optional. */
  exchange?: string;
  /** Drives the colored dot under the day cell + the priority pill. */
  priority?: DeadlinePriority | null;
  /** If present, the title renders as <a href> instead of plain text. */
  url?: string;
}

@Component({
  selector: 'app-deadline-calendar',
  standalone: true,
  imports: [CommonModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './deadline-calendar.component.html',
  styleUrl: './deadline-calendar.component.css',
})
export class DeadlineCalendarComponent {
  // ── Inputs ────────────────────────────────────────────────
  @Input() events: DeadlineEvent[] = [];
  @Input() disabled = false;
  @Input() title = 'Deadline Calendar';
  @Input() maxItemsVisible = 2;
  @Input() viewDate: Date = new Date();
  @Input() showItemList = true;

  // ── Outputs ───────────────────────────────────────────────
  @Output() daySelected = new EventEmitter<Date>();
  @Output() viewAll = new EventEmitter<void>();
  @Output() eventClicked = new EventEmitter<DeadlineEvent>();
  @Output() monthChange = new EventEmitter<string>();

  // ── State ─────────────────────────────────────────────────
  currentMonth = signal<Date>(new Date());
  selectedDay = signal<Date | null>(null);

  // ── Constants ─────────────────────────────────────────────
  readonly weekDays = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'] as const;

  // ── Computed ──────────────────────────────────────────────
  calendarDays = computed(() => {
    const month = this.currentMonth();
    const start = startOfWeek(startOfMonth(month), { weekStartsOn: 1 });
    const end = endOfWeek(endOfMonth(month), { weekStartsOn: 1 });
    return eachDayOfInterval({ start, end });
  });

  selectedDayEvents = computed<DeadlineEvent[]>(() => {
    const day = this.selectedDay();
    if (!day) return [];
    return this.events.filter((e) => isSameDay(new Date(e.date), day));
  });

  visibleSelectedDayEvents = computed<DeadlineEvent[]>(() => {
    return this.selectedDayEvents().slice(0, this.maxItemsVisible);
  });

  overflowCount = computed(() => {
    return Math.max(0, this.selectedDayEvents().length - this.maxItemsVisible);
  });

  // ── Helpers ──────────────────────────────────────────────
  format(date: Date, pattern: string): string {
    return format(date, pattern);
  }

  hasEventsOn(date: Date): boolean {
    return this.events.some((e) => isSameDay(new Date(e.date), date));
  }

  /** Up to 3 distinct priority colors for the indicator dots under a day. */
  priorityDotsForDay(date: Date): DeadlinePriority[] {
    const items = this.events.filter((e) => isSameDay(new Date(e.date), date));
    const seen = new Set<DeadlinePriority>();
    const out: DeadlinePriority[] = [];
    for (const item of items) {
      const p = (item.priority ?? 'medium') as DeadlinePriority;
      if (!seen.has(p)) {
        seen.add(p);
        out.push(p);
        if (out.length >= 3) break;
      }
    }
    return out;
  }

  // ── Actions ──────────────────────────────────────────────
  selectDay(date: Date): void {
    if (this.disabled) return;
    this.selectedDay.set(date);
    this.daySelected.emit(date);
  }

  prevMonth(): void {
    if (this.disabled) return;
    const next = subMonths(this.currentMonth(), 1);
    this.currentMonth.set(next);
    this.selectedDay.set(null);
    this.monthChange.emit(format(next, 'MM-yyyy')); // "10-2026"
  }

  nextMonth(): void {
    if (this.disabled) return;
    const next = addMonths(this.currentMonth(), 1);
    this.currentMonth.set(next);
    this.selectedDay.set(null);
    this.monthChange.emit(format(next, 'MM-yyyy'));
  }

  onViewAll(event: Event): void {
    if (this.disabled) {
      event.preventDefault();
      return;
    }
    event.preventDefault();
    this.viewAll.emit();
  }

  onEventClick(event: DeadlineEvent, ev: MouseEvent): void {
    if (this.disabled) {
      ev.preventDefault();
      return;
    }
    this.eventClicked.emit(event);
  }

  // Hoist date-fns helpers into the template's scope
  isSameMonthFn = isSameMonth;
  isTodayFn = isToday;
  isSameDayFn = isSameDay;
}
