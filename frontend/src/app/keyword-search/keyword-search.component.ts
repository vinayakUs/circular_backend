import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { Subject, debounceTime, distinctUntilChanged } from 'rxjs';

import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, SearchResult } from '../services/circulars-api.service';

type ExchangeTab = 'ALL' | 'NSE' | 'SEBI';
type SortOption = 'score' | 'date';

interface SortTab {
  value: SortOption;
  label: string;
}

@Component({
  selector: 'app-keyword-search',
  imports: [NavbarComponent, CommonModule, FormsModule],
  templateUrl: './keyword-search.component.html',
  styleUrl: './keyword-search.component.css',
  standalone: true
})
export class KeywordSearchComponent implements OnInit {
  private apiService = inject(CircularsApiService);
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private sanitizer = inject(DomSanitizer);

  query = '';
  source: ExchangeTab = 'ALL';
  sort: SortOption = 'score';
  results: SearchResult[] = [];
  loading = false;
  error = false;
  hasSearched = false;

  readonly exchanges: ExchangeTab[] = ['ALL', 'NSE', 'SEBI'];
  readonly sortOptions: SortTab[] = [
    { value: 'score', label: 'Most Relevant' },
    { value: 'date', label: 'Newest First' },
  ];

  private queryInput$ = new Subject<string>();
  private searchToken = 0;

  ngOnInit(): void {
    this.route.queryParamMap.subscribe(params => {
      const q = (params.get('q') ?? '').toString();
      this.query = q;
      if (q) {
        this.runSearch();
      }
    });

    this.queryInput$
      .pipe(debounceTime(250), distinctUntilChanged())
      .subscribe(() => this.runSearch());
  }

  onQueryChange(value: string): void {
    this.query = value;
    this.queryInput$.next(value);
  }

  onSubmit(): void {
    this.runSearch();
  }

  onSourceChange(value: ExchangeTab): void {
    if (this.source === value) return;
    this.source = value;
    this.runSearch();
  }

  onSortChange(value: SortOption): void {
    if (this.sort === value) return;
    this.sort = value;
    this.runSearch();
  }

  onResultClick(result: SearchResult): void {
    this.router.navigate(['/circular', result.id]);
  }

  exchangeLabel(value: ExchangeTab): string {
    return value === 'ALL' ? 'All' : value;
  }

  badgeClass(source: string): string {
    return (source || '').toLowerCase();
  }

  safePreview(preview: string | undefined): SafeHtml {
    // Backend-built fallback preview (HTML with <mark> tags). Used only when
    // ES highlights are unavailable.
    return this.sanitizer.bypassSecurityTrustHtml(preview ?? '');
  }

  safeHighlight(highlights: Record<string, string[]> | null | undefined): SafeHtml {
    // ES returns the highlighted fragment wrapped in <mark>...</mark> tags
    // (configured via pre_tags/post_tags on the bm25v2 search query).
    // The unified highlighter also escapes other HTML special characters in
    // the source text, so the result is render-ready HTML.
    const fragment = highlights?.['chunk_text']?.[0];
    if (fragment) {
      return this.sanitizer.bypassSecurityTrustHtml(fragment);
    }
    return this.safePreview(undefined);
  }

  runSearch(): void {
    const q = this.query.trim();
    if (!q) {
      this.results = [];
      this.hasSearched = false;
      this.loading = false;
      this.error = false;
      return;
    }

    const token = ++this.searchToken;
    this.loading = true;
    this.error = false;
    this.hasSearched = true;

    this.apiService.keywordSearchV2({
      query: q,
      source: this.source === 'ALL' ? undefined : this.source,
      sort: this.sort,
    }).subscribe({
      next: (response) => {
        if (token !== this.searchToken) return;
        this.results = response.results ?? [];
        this.loading = false;
      },
      error: () => {
        if (token !== this.searchToken) return;
        this.results = [];
        this.loading = false;
        this.error = true;
      }
    });
  }
}
