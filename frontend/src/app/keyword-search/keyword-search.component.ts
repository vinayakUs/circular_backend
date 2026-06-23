import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';

import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, SearchResult, SemanticSearchResponse } from '../services/circulars-api.service';

type ExchangeTab = 'ALL' | 'NSE' | 'SEBI';
type SortOption = 'score' | 'date';
type SearchMode = 'keyword' | 'semantic';

interface SortTab {
  value: SortOption;
  label: string;
}

interface ModeTab {
  value: SearchMode;
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
  mode: SearchMode = 'keyword';
  source: ExchangeTab = 'ALL';
  sort: SortOption = 'score';
  results: SearchResult[] = [];
  loading = false;
  error = false;
  hasSearched = false;

  // Semantic search state
  semanticResult: SemanticSearchResponse | null = null;
  semanticLoading = false;
  semanticError = false;

  readonly exchanges: ExchangeTab[] = ['ALL', 'NSE', 'SEBI'];
  readonly sortOptions: SortTab[] = [
    { value: 'score', label: 'Most Relevant' },
    { value: 'date', label: 'Newest First' },
  ];
  readonly modes: ModeTab[] = [
    { value: 'keyword', label: 'Keyword' },
    { value: 'semantic', label: 'Semantic' },
  ];

  private searchToken = 0;

  ngOnInit(): void {
    this.route.queryParamMap.subscribe(params => {
      const q = (params.get('q') ?? '').toString();
      const modeParam = (params.get('mode') ?? '').toString();
      this.query = q;
      if (modeParam === 'semantic' || modeParam === 'keyword') {
        this.mode = modeParam;
      }
      if (q) {
        this.runSearch();
      }
    });
  }

  onQueryChange(value: string): void {
    this.query = value;
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

  onModeChange(value: SearchMode): void {
    if (this.mode === value) return;
    this.mode = value;
    this.semanticError = false;
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

  safeMarkdown(text: string | null | undefined): SafeHtml {
    if (!text) return '';
    const html = marked.parse(text, { async: false }) as string;
    return this.sanitizer.bypassSecurityTrustHtml(html);
  }

  get isKeywordMode(): boolean {
    return this.mode === 'keyword';
  }

  get isSemanticMode(): boolean {
    return this.mode === 'semantic';
  }

  get showKeywordResults(): boolean {
    return this.isKeywordMode && this.results.length > 0;
  }

  get showKeywordEmpty(): boolean {
    return this.isKeywordMode
      && !this.loading
      && this.hasSearched
      && this.results.length === 0
      && !this.error;
  }

  get showKeywordInitial(): boolean {
    return this.isKeywordMode && !this.hasSearched && !this.loading;
  }

  get showKeywordError(): boolean {
    return this.isKeywordMode && this.error;
  }

  get showSemanticAnswer(): boolean {
    return this.isSemanticMode
      && !this.semanticLoading
      && !!this.semanticResult
      && !this.semanticError;
  }

  get showSemanticLoading(): boolean {
    return this.isSemanticMode && this.semanticLoading;
  }

  get showSemanticEmpty(): boolean {
    return this.isSemanticMode
      && !this.semanticLoading
      && this.hasSearched
      && !this.semanticResult
      && !this.semanticError;
  }

  get showSemanticInitial(): boolean {
    return this.isSemanticMode && !this.hasSearched && !this.semanticLoading;
  }

  get showSemanticError(): boolean {
    return this.isSemanticMode && this.semanticError;
  }

  runSearch(): void {
    const q = this.query.trim();
    if (!q) {
      this.results = [];
      this.semanticResult = null;
      this.hasSearched = false;
      this.loading = false;
      this.error = false;
      this.semanticLoading = false;
      this.semanticError = false;
      return;
    }

    if (this.isSemanticMode) {
      this.runSemanticSearch();
    } else {
      this.runKeywordSearch();
    }
  }

  private runKeywordSearch(): void {
    const token = ++this.searchToken;
    this.loading = true;
    this.error = false;
    this.hasSearched = true;
    this.semanticResult = null;

    this.apiService.keywordSearchV2({
      query: this.query.trim(),
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

  private runSemanticSearch(): void {
    const token = ++this.searchToken;
    this.semanticLoading = true;
    this.semanticError = false;
    this.hasSearched = true;
    this.results = [];

    this.apiService.semanticSearch({
      query: this.query.trim(),
      strategy: 'hybrid',
      source: this.source === 'ALL' ? undefined : this.source,
    }).subscribe({
      next: (response) => {
        if (token !== this.searchToken) return;
        this.semanticResult = response;
        this.semanticLoading = false;
      },
      error: () => {
        if (token !== this.searchToken) return;
        this.semanticResult = null;
        this.semanticLoading = false;
        this.semanticError = true;
      }
    });
  }
}
