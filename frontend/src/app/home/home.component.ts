import { Component, OnInit, inject } from '@angular/core';
import { DecimalPipe, DatePipe, NgIf, NgFor } from '@angular/common';
import { Router } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, Circular, PaginatedCircularsResponse, CountsResponse } from '../services/circulars-api.service';

@Component({
  selector: 'app-home',
  standalone: true,
  imports: [NavbarComponent, DecimalPipe, DatePipe, NgIf, NgFor, FormsModule],
  templateUrl: './home.component.html',
  styleUrl: './home.component.css'
})
export class HomeComponent implements OnInit {
  private apiService = inject(CircularsApiService);
  private router = inject(Router);
  counts: CountsResponse | null = null;
  circulars: Circular[] = [];
  loadingCounts = true;
  loadingCirculars = true;
  isApiError = false;
  searchQuery = '';

  ngOnInit(): void {
    this.apiService.getCounts().subscribe({
      next: (data) => {
        this.counts = data;
        this.loadingCounts = false;
      },
      error: () => {
        this.counts = null;
        this.loadingCounts = false;
      }
    });

    this.apiService.getLatestCirculars().subscribe({
      next: (data: PaginatedCircularsResponse) => {
        this.circulars = data.data.circulars;
        this.loadingCirculars = false;
        this.isApiError = false;
      },
      error: () => {
        this.circulars = [];
        this.loadingCirculars = false;
        this.isApiError = true;
      }
    });
  }

  navigateToCircular(circularId: string): void {
    this.router.navigate(['/circular', circularId]);
  }

  onSearch(): void {
    if (this.searchQuery.trim()) {
      this.router.navigate(['/all-circulars'], {
        queryParams: { q: this.searchQuery.trim() }
      });
    }
  }
}
