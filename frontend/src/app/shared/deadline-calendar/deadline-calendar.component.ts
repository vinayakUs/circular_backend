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

/** Most dots rendered under one day cell. Three fits the 40px cell height;
 *  beyond that the count would stop being readable at a glance. */
const MAX_DAY_DOTS = 3;

export interface DeadlineEvent {
  /** Backend-generated UUID. */
  id: UUID;
  /** Day this event occurs on. ISO string or Date — both accepted. */
  date: Date | string;
  /** The action-item text describing the deadline obligation. */
  title: string;
  /** Exchange badge (e.g. 'NSE', 'SEBI'). Optional. */
  exchange?: string;
  /** If present, the title renders as <a href> instead of plain text. */
  url?: string;
  /** Title of the source circular this action item was extracted from. */
  circular_title?: string;
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

  /** One indicator dot per deadline on this day, capped so a busy day
   *  stays the same height as a quiet one instead of stretching the row. */
  dotsForDay(date: Date): number[] {
    const count = this.events.filter((e) => isSameDay(new Date(e.date), date)).length;
    return Array.from({ length: Math.min(count, MAX_DAY_DOTS) });
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
