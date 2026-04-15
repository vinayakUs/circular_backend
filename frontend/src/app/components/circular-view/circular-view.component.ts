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

  readonly actionButtons: ActionButton[] = [
    { label: 'Download PDF', variant: 'primary', action: 'pdf' },
    { label: 'View on Site', variant: 'secondary', action: 'source' },
  ];

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
