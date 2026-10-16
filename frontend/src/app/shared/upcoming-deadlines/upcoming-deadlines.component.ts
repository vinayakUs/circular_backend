import { CommonModule } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  Output,
} from '@angular/core';
import { format } from 'date-fns';

import { DeadlineEvent } from '../deadline-calendar/deadline-calendar.component';

/**
 * Lightweight list-style widget that shows the next N upcoming deadlines
 * alongside the calendar. Reuses the calendar's `DeadlineEvent` shape so
 * the parent can hand in the same data it already loads.
 */
@Component({
  selector: 'app-upcoming-deadlines',
  standalone: true,
  imports: [CommonModule],
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './upcoming-deadlines.component.html',
  styleUrl: './upcoming-deadlines.component.css',
})
export class UpcomingDeadlinesComponent {
  @Input() events: DeadlineEvent[] = [];
  @Input() title = 'Upcoming Deadlines';
  @Input() disabled = false;
  @Input() loading = false;
  @Input() emptyMessage = 'No upcoming deadlines.';

  @Output() viewAll = new EventEmitter<void>();
  @Output() eventClicked = new EventEmitter<DeadlineEvent>();

  formatDate(date: Date | string): string {
    return format(new Date(date), 'd MMM yyyy');
  }

  onViewAll(event: Event): void {
    if (this.disabled) {
      event.preventDefault();
      return;
    }
    event.preventDefault();
    this.viewAll.emit();
  }

  onEventClick(item: DeadlineEvent, ev: MouseEvent): void {
    if (this.disabled) {
      ev.preventDefault();
      return;
    }
    // Always preventDefault so the parent controls navigation
    // (avoids a full page reload via <a href>).
    ev.preventDefault();
    this.eventClicked.emit(item);
  }
}
