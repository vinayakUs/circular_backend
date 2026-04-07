import { HttpClient } from '@angular/common/http';
import { Component, inject } from '@angular/core';
import { FormsModule } from '@angular/forms';

type CircularCountsResponse = {
  nse: number;
  sebi: number;
  total: number;
};

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [FormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent {
  private readonly http = inject(HttpClient);

  searchQuery = '';
  searchPlaceholder = 'Search by NSE circular, SEBI reference, or topic';
  statsError = false;

  highlights = [
    'NSE + SEBI tracking',
    'Amendment history',
    'AI-assisted search'
  ];

  stats = [
    { value: '—', label: 'Circulars tracked' },
    { value: '—', label: 'NSE notices' },
    { value: '—', label: 'SEBI circulars' },
    { value: '24/7', label: 'Monitoring window' }
  ];

  constructor() {
    this.loadCounts();
  }

  private loadCounts(): void {
    this.http.get<CircularCountsResponse>('/api/circulars/counts').subscribe({
      next: (counts) => {
        this.stats = [
          { value: this.formatNumber(counts.total), label: 'Circulars tracked' },
          { value: this.formatNumber(counts.nse), label: 'NSE notices' },
          { value: this.formatNumber(counts.sebi), label: 'SEBI circulars' },
          { value: '24/7', label: 'Monitoring window' }
        ];
      },
      error: () => {
        this.statsError = true;
      }
    });
  }

  private formatNumber(value: number): string {
    return new Intl.NumberFormat('en-IN').format(value);
  }
}
