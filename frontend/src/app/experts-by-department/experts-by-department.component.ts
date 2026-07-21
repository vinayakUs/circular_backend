import { Component, OnInit, inject } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, Department, ExpertWithCircular } from '../services/circulars-api.service';

@Component({
  selector: 'app-experts-by-department',
  standalone: true,
  imports: [CommonModule, NavbarComponent, DatePipe, FormsModule],
  templateUrl: './experts-by-department.component.html',
  styleUrl: './experts-by-department.component.css'
})
export class ExpertsByDepartmentComponent implements OnInit {
  private apiService = inject(CircularsApiService);
  private router = inject(Router);

  departments: Department[] = [];
  selectedDepartmentId: string | null = null;
  experts: ExpertWithCircular[] = [];
  loading = false;

  filters = {
    source: '' as '' | 'NSE' | 'SEBI',
    status: '' as '' | 'open' | 'closed',
    from_date: '',
    to_date: '',
    full_circular_no: ''
  };

  pagination = {
    page: 1,
    page_size: 20,
    total: 0,
    total_pages: 0
  };

  ngOnInit(): void {
    this.loadDepartments();
    this.loadExperts();
  }

  loadDepartments(): void {
    this.apiService.getAvailableDepartments().subscribe({
      next: (data) => {
        this.departments = data.items.filter(d => !d.archived);
      }
    });
  }

  loadExperts(): void {
    this.loading = true;
    this.apiService.getExpertsByDepartment({
      department_id: this.selectedDepartmentId || undefined,
      source: this.filters.source || undefined,
      status: this.filters.status || undefined,
      from_date: this.filters.from_date || undefined,
      to_date: this.filters.to_date || undefined,
      full_circular_no: this.filters.full_circular_no || undefined,
      page: this.pagination.page,
      page_size: this.pagination.page_size
    }).subscribe({
      next: (data) => {
        this.experts = data.experts.items;
        this.pagination.total = data.experts.total;
        this.pagination.total_pages = data.experts.total_pages;
        this.loading = false;
      },
      error: () => {
        this.experts = [];
        this.loading = false;
      }
    });
  }

  selectDepartment(deptId: string): void {
    this.selectedDepartmentId = this.selectedDepartmentId === deptId ? null : deptId;
    this.pagination.page = 1;
    this.loadExperts();
  }

  applyFilters(): void {
    this.pagination.page = 1;
    this.loadExperts();
  }

  setStatusFilter(status: '' | 'open' | 'closed'): void {
    this.filters.status = status;
    this.pagination.page = 1;
    this.loadExperts();
  }

  clearFilters(): void {
    this.filters = { source: '', status: '', from_date: '', to_date: '', full_circular_no: '' };
    this.pagination.page = 1;
    this.loadExperts();
  }

  goToPage(page: number): void {
    if (page < 1 || page > this.pagination.total_pages) return;
    this.pagination.page = page;
    this.loadExperts();
  }

  navigateToTaskview(circularId: string, expertId: string): void {
    this.router.navigate(
      ['/taskview', circularId],
      { queryParams: { expertId } }
    );
  }

  get pageNumbers(): number[] {
    const pages: number[] = [];
    const total = this.pagination.total_pages;
    const current = this.pagination.page;

    if (total <= 7) {
      for (let i = 1; i <= total; i++) pages.push(i);
    } else {
      pages.push(1);
      if (current > 3) pages.push(-1);
      for (let i = Math.max(2, current - 1); i <= Math.min(total - 1, current + 1); i++) {
        pages.push(i);
      }
      if (current < total - 2) pages.push(-1);
      pages.push(total);
    }
    return pages;
  }
}