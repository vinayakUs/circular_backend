import { Component, OnInit, inject } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { marked } from 'marked';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, Circular, SemanticSearchResponse, SearchResult } from '../services/circulars-api.service';

@Component({
  selector: 'app-all-circulars',
  standalone: true,
  imports: [CommonModule, NavbarComponent, DatePipe, FormsModule],
  templateUrl: './all-circulars.component.html',
  styleUrl: './all-circulars.component.css'
})
export class AllCircularsComponent implements OnInit {
  private apiService = inject(CircularsApiService);
  private sanitizer = inject(DomSanitizer);

  circulars: Circular[] = [];
  searchResults: SearchResult[] = [];
  total = 0;
  loading = true;
  isApiError = false;

  filters = {
    source: 'ALL',
    from_date: '',
    to_date: '',
    search: ''
  };

  searchType: 'keyword' | 'semantic' = 'keyword';

  // Semantic search state
  semanticResult: SemanticSearchResponse | null = null;
  semanticLoading = false;
  showSemanticResult = false;

  pagination = {
    limit: 10,
    offset: 0
  };

  ngOnInit(): void {
    this.loadCirculars();
  }

  loadCirculars(): void {
    this.loading = true;
    this.apiService.getCirculars({
      source: this.filters.source,
      limit: this.pagination.limit,
      offset: this.pagination.offset,
      from_date: this.filters.from_date || undefined,
      to_date: this.filters.to_date || undefined,
      search: this.filters.search || undefined
    }).subscribe({
      next: (data) => {
        this.circulars = data.items;
        this.total = data.total;
        this.loading = false;
        this.isApiError = false;
      },
      error: () => {
        this.circulars = [];
        this.total = 0;
        this.loading = false;
        this.isApiError = true;
      }
    });
  }

  setSearchType(type: 'keyword' | 'semantic'): void {
    this.searchType = type;
    if (type === 'keyword') {
      this.showSemanticResult = false;
      this.semanticResult = null;
    }
  }

  onSearch(): void {
    this.pagination.offset = 0;
    this.showSemanticResult = false;
    this.semanticResult = null;
    if (this.searchType === 'semantic') {
      this.performSemanticSearch();
    } else {
      this.performKeywordSearch();
    }
  }

  performKeywordSearch(): void {
    if (!this.filters.search.trim()) {
      this.loadCirculars();
      return;
    }

    this.loading = true;
    this.apiService.keywordSearch({
      query: this.filters.search,
      source: this.filters.source === 'ALL' ? undefined : this.filters.source,
      from_date: this.filters.from_date || undefined,
      to_date: this.filters.to_date || undefined
    }).subscribe({
      next: (data) => {
        this.searchResults = data.results;
        this.circulars = data.results.map(r => r.document);
        this.total = data.results.length;
        this.loading = false;
        this.isApiError = false;
      },
      error: () => {
        this.circulars = [];
        this.total = 0;
        this.loading = false;
        this.isApiError = true;
      }
    });
  }

  performSemanticSearch(): void {
    if (!this.filters.search.trim()) return;

    this.semanticLoading = true;
    this.semanticResult = null;
    this.showSemanticResult = true;

    this.apiService.semanticSearch({
      query: this.filters.search,
      strategy: 'hybrid',
      source: this.filters.source === 'ALL' ? undefined : this.filters.source,
      from_date: this.filters.from_date || undefined,
      to_date: this.filters.to_date || undefined
    }).subscribe({
      next: (data) => {
        this.semanticResult = data;
        this.semanticLoading = false;
        this.isApiError = false;
      },
      error: () => {
        this.semanticResult = null;
        this.semanticLoading = false;
        this.isApiError = true;
      }
    });
  }

  onFilterChange(source: string): void {
    this.filters.source = source;
  }

  clearFilters(): void {
    this.filters = { source: 'ALL', from_date: '', to_date: '', search: '' };
    this.pagination.offset = 0;
    this.loadCirculars();
  }

  goToPage(offset: number): void {
    this.pagination.offset = offset;
    this.loadCirculars();
  }

  get totalPages(): number {
    return Math.ceil(this.total / this.pagination.limit);
  }

  get currentPage(): number {
    return Math.floor(this.pagination.offset / this.pagination.limit) + 1;
  }

  get pageNumbers(): number[] {
    const pages: number[] = [];
    const total = this.totalPages;
    const current = this.currentPage;

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

  parseMarkdown(text: string): string {
    if (!text) return '';
    return marked.parse(text) as string;
  }

  getPreview(result: SearchResult): SafeHtml {
    return this.sanitizer.bypassSecurityTrustHtml(result.preview);
  }

  clearSearch(): void {
    this.searchResults = [];
    this.filters.search = '';
    this.pagination.offset = 0;
    this.loadCirculars();
  }
}