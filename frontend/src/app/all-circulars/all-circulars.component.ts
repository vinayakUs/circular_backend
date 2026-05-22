import { Component, OnInit, inject } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { Router, ActivatedRoute } from '@angular/router';
import { marked } from 'marked';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, Circular, SemanticSearchResponse, SearchResult, SearchResponse } from '../services/circulars-api.service';

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
  private router = inject(Router);
  private route = inject(ActivatedRoute);

  circulars: Circular[] = [];
  searchResults: SearchResult[] = [];
  total = 0;
  loading = true;
  isApiError = false;

  filters = {
    source: 'ALL',
    from_date: '',
    to_date: '',
    search: '',
    applicable_to_nse: null as boolean | null,
    signatory: ''
  };

  searchType: 'browse' | 'keyword' | 'semantic' = 'browse';
  signatoryOptions: string[] = [];

  // Semantic search state
  semanticResult: SemanticSearchResponse | null = null;
  semanticLoading = false;
  showSemanticResult = false;

  pagination = {
    limit: 10,
    offset: 0
  };

  ngOnInit(): void {
    this.loadSignatoryOptions();
    this.route.queryParams.subscribe(params => {
      if (params['q']) {
        this.filters.search = params['q'];
        this.searchType = 'keyword';
        this.performKeywordSearch();
      } else {
        this.loadCirculars();
      }
    });
  }

  loadSignatoryOptions(): void {
    this.apiService.getAvailableSignatories().subscribe({
      next: (data) => {
        this.signatoryOptions = data.items.map(i => i.name);
      },
      error: () => {
        this.signatoryOptions = [];
      }
    });
  }

  loadCirculars(): void {
    this.loading = true;
    this.searchResults = [];
    this.apiService.getCirculars({
      source: this.filters.source,
      limit: this.pagination.limit,
      offset: this.pagination.offset,
      from_date: this.filters.from_date || undefined,
      to_date: this.filters.to_date || undefined,
      search: this.filters.search || undefined,
      applicable_to_nse: this.filters.applicable_to_nse,
      signatory: this.filters.signatory || undefined
    }).subscribe({
      next: (data) => {
        this.circulars = data.data.circulars;
        this.total = data.pagination.total;
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

  setSearchType(type: 'browse' | 'keyword' | 'semantic'): void {
    this.searchType = type;
    this.searchResults = [];
    this.circulars = [];
    this.semanticResult = null;
    this.total = 0;
    this.showSemanticResult = false;
    if (type === 'browse') {
      this.loadCirculars();
    }
  }

  onSearch(): void {
    this.pagination.offset = 0;
    this.showSemanticResult = false;
    this.semanticResult = null;
    if (this.searchType === 'semantic') {
      this.performSemanticSearch();
    } else if (this.searchType === 'keyword') {
      this.performKeywordSearch();
    } else {
      this.loadCirculars();
    }
  }

  performKeywordSearch(): void {
    console.log("Performing keyword search with query:", this.filters.search);
    if (!this.filters.search.trim()) {
      this.searchResults = [];
      // this.loadCirculars();
      this.total=0;
      return;
    }

    this.loading = true;
    this.apiService.keywordSearch({
      query: this.filters.search,
      source: this.filters.source === 'ALL' ? undefined : this.filters.source,
      from_date: this.filters.from_date || undefined,
      to_date: this.filters.to_date || undefined,
      applicable_to_nse: this.filters.applicable_to_nse
    }).subscribe({
      next: (data: SearchResponse) => {
        console.log("data:", data);
        this.searchResults = data.results;
        console.log("searchResults:", this.searchResults);

        this.circulars = data.results.map(r => ({
          id: r.id,
          circular_id: r.circularId,
          full_reference: r.fullReference,
          department: r.department,
          source: r.source,
          title: r.title,
          issue_date: r.issueDate,
          applicable_to_nse: r.applicableToNse ?? false,
          status: '',
          url: r.url,
          signatories: []
        }));
        console.log("circulars:", this.circulars);

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
    this.searchResults = [];
    this.circulars = [];
    if (!this.filters.search.trim()) return;

    this.semanticLoading = true;
    this.semanticResult = null;
    this.showSemanticResult = true;
    this.total = 0;

    this.apiService.semanticSearch({
      query: this.filters.search,
      strategy: 'hybrid',
      source: this.filters.source === 'ALL' ? undefined : this.filters.source,
      from_date: this.filters.from_date || undefined,
      to_date: this.filters.to_date || undefined,
      applicable_to_nse: this.filters.applicable_to_nse
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

  onApplicableToNseChange(value: boolean | null): void {
    this.filters.applicable_to_nse = value;
  }

  clearFilters(): void {
    const savedQuery = this.filters.search;
    this.filters = { source: 'ALL', from_date: '', to_date: '', search: savedQuery, applicable_to_nse: null, signatory: '' };
    this.searchResults = [];
    this.showSemanticResult = false;
    this.pagination.offset = 0;
    this.onSearch();
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

  navigateToCircular(circularId: string,circ: any): void {
    console.log("Navigating to circular:", circularId, circ);
    this.router.navigate(['/circular', circularId]);
  }

  clearSearch(): void {
    this.searchResults = [];
    this.filters.search = '';
    this.pagination.offset = 0;
    this.loadCirculars();
  }
}