import { CommonModule } from '@angular/common';
import { Component, inject, signal } from '@angular/core';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { map, switchMap } from 'rxjs/operators';
import { EMPTY } from 'rxjs';
import { ToastrService } from 'ngx-toastr';
 import { IndexService } from '../../services/index.service';

interface CircularViewModel {
  recordId: string;
  source: string;
  circularId: string;
  fullReference: string;
  title: string;
  issueDate: string;
  department: string;
  summary: string;
  sourceUrl: string;
  pdfUrl: string;
}

type CircularAction = 'pdf' | 'source';

interface ActionButton {
  label: string;
  variant: 'primary' | 'secondary';
  action: CircularAction;
}
export interface ActionItem {
  action_item: string;
  circular_id: string; // UUID
  created_at: string; // ISO timestamp
  deadline: string; // ISO date (YYYY-MM-DD)
  id: string; // UUID
  priority: "low" | "medium" | "high";
  updated_at: string; // ISO timestamp
}
export interface ActionItemsResponse {
  action_items: ActionItem[];
  limit: number;
  offset: number;
  total: number;
}

@Component({
  selector: 'app-circular-view',
  standalone: true,
  imports: [CommonModule, RouterLink],
  templateUrl: './circular-view.component.html',
  styleUrl: './circular-view.component.css'
})
export class CircularViewComponent {
  private readonly route = inject(ActivatedRoute);
  private readonly indexService = inject(IndexService);
  private readonly toastr = inject(ToastrService);
  readonly isLoading = signal(true);
  readonly hasError = signal(false);
  currentDate = new Date();
  readonly circular = signal<CircularViewModel>({
    recordId: '',
    source: '',
    circularId: '',
    fullReference: '',
    title: 'Loading circular...',
    issueDate: '',
    department: '',
    summary: '',
    sourceUrl: '#',
    pdfUrl: '#'
  });

  readonly actionItems = signal<ActionItem[]>([]);


  readonly actionButtons: ActionButton[] = [
    { label: 'Download PDF', variant: 'primary', action: 'pdf' },
    { label: 'View on Site', variant: 'secondary', action: 'source' },
  ];

 smartDateDiff(d1: Date, d2: Date): string {
  const diffMs = Math.abs(d2.getTime() - d1.getTime());

  const minutes = diffMs / (1000 * 60);
  const hours = diffMs / (1000 * 60 * 60);
  const days = diffMs / (1000 * 60 * 60 * 24);

  if (days >= 1) {
    return `${Math.floor(days)} day(s)`;
  } else if (hours >= 1) {
    return `${Math.floor(hours)} hour(s)`;
  } else {
    return `${Math.floor(minutes)} minute(s)`;
  }
}
 parseLocalDate(dateStr: string): Date {
  const [year, month, day] = dateStr.split("-").map(Number);
  return new Date(year, month - 1, day); // local time, no timezone shift
}
  constructor() {
    this.route.paramMap.pipe(
      map((params) => params.get('id')?.trim() || ''),
      switchMap((recordId) => {
        if (!recordId) {
          this.hasError.set(true);
          this.isLoading.set(false);
          return EMPTY;
        }

        this.isLoading.set(true);
        this.hasError.set(false);

        return this.indexService.getCircularDetail(recordId);
      })
    ).subscribe({
      next: (response) => {
        const detail = response.circular;

        this.circular.set({
          recordId: detail.id,
          source: detail.source,
          circularId: detail.circular_id,
          fullReference: detail.full_reference,
          title: detail.title,
          issueDate: this.formatDate(detail.issue_date),
          department: detail.department,
          summary: detail.status,
          sourceUrl: detail.url || '#',
          pdfUrl: detail.pdf_url || '#'
        });

        this.indexService.getActionItems(detail.id).subscribe({
          next: (actionResponse) => {
            this.actionItems.set(actionResponse.action_items);
          },
          error: () => {
            this.toastr.error('Failed to fetch action items. Please try again later.', 'Error');
          }
        });



        this.isLoading.set(false);
      },
      error: () => {
        this.hasError.set(true);
        this.isLoading.set(false);
        this.toastr.error('Failed to fetch circular details. Please try again later.', 'Error');
      }
    });
  }

  openAction(action: CircularAction): void {
    const circular = this.circular();
    const targetUrl = action === 'pdf' ? circular.pdfUrl : circular.sourceUrl;

    if (!targetUrl || targetUrl === '#') {
      return;
    }

    window.open(targetUrl, '_blank', 'noopener,noreferrer');
  }

  private formatDate(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime())
      ? value
      : new Intl.DateTimeFormat('en-GB', {
          day: '2-digit',
          month: 'short',
          year: 'numeric'
        }).format(date);
  }
}
