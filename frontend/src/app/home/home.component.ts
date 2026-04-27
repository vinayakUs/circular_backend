import { Component, OnInit, inject } from '@angular/core';
import { DecimalPipe, DatePipe, NgIf, NgFor } from '@angular/common';
import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, CircularsResponse, CountsResponse } from '../services/circulars-api.service';

@Component({
  selector: 'app-home',
  standalone: true,
  imports: [NavbarComponent, DecimalPipe, DatePipe, NgIf, NgFor],
  templateUrl: './home.component.html',
  styleUrl: './home.component.css'
})
export class HomeComponent implements OnInit {
  private apiService = inject(CircularsApiService);
  counts: CountsResponse | null = null;
  circulars: CircularsResponse | null = null;
  loadingCounts = true;
  loadingCirculars = true;
  isApiError = false;

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
      next: (data) => {
        this.circulars = data;
        this.loadingCirculars = false;
        this.isApiError = false;
      },
      error: () => {
        this.circulars = null;
        this.loadingCirculars = false;
        this.isApiError = true;
      }
    });
  }
}
