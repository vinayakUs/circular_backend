import { Component, OnInit, HostListener, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { DomSanitizer, SafeHtml } from '@angular/platform-browser';
import { marked } from 'marked';

import { NavbarComponent } from '../navbar/navbar.component';
import { CircularsApiService, SearchResult, SemanticSearchResponse } from '../services/circulars-api.service';
import { RecentSearchesService, RecentSearch, SearchMode } from '../services/recent-searches.service';
import { ExchangeSource, exchangeLabel } from '../util/source.util';

type SortOption = 'score' | 'date';
type ModeOption = 'keyword' | 'semantic' | 'phrase' | 'title';

interface SortTab {
  value: SortOption;
  label: string;
}

interface ModeTab {
  value: ModeOption;
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
  private recentSearches = inject(RecentSearchesService);

  query = '';
  mode: ModeOption = 'keyword';
  source: ExchangeSource = ExchangeSource.ALL;
  sort: SortOption = 'score';
  results: SearchResult[] = [];
  loading = false;
  error = false;
  hasSearched = false;

  // Semantic search state
  semanticResult: SemanticSearchResponse | null = null;
  semanticLoading = false;
  semanticError = false;

  readonly exchanges = Object.keys(ExchangeSource) as (keyof typeof ExchangeSource)[];
  readonly sortOptions: SortTab[] = [
    { value: 'score', label: 'Most Relevant' },
    { value: 'date', label: 'Newest First' },
  ];
  readonly modes: ModeTab[] = [
    { value: 'keyword', label: 'Keyword Search' },
    { value: 'semantic', label: 'Semantic Search' },
    { value: 'phrase', label: 'Phrase Search' },
    { value: 'title', label: 'Title Search' },
  ];

  private searchToken = 0;

  // ── Recent searches dropdown ────────────────────────────────
  showRecentSearches = false;
  recentSearchesList: RecentSearch[] = [];
  private recentSearchBlurTimeout: ReturnType<typeof setTimeout> | null = null;

  // ── Mode menu (replaces native <select>) ────────────────────
  showModeMenu = false;

  ngOnInit(): void {
    this.recentSearchesList = this.recentSearches.getRecent();

    this.route.queryParamMap.subscribe(params => {
      const q = (params.get('q') ?? '').toString();
      const modeParam = (params.get('mode') ?? '').toString();
      this.query = q;
      if (
        modeParam === 'semantic'
        || modeParam === 'keyword'
        || modeParam === 'phrase'
        || modeParam === 'title'
      ) {
        this.mode = modeParam;
      }
      if (q) {
        // A query arriving via the URL counts as a search worth remembering —
        // this is how links from elsewhere in the app land here. Recorded
        // against the mode the link asked for, not the previous one.
        this.recentSearchesList = this.recentSearches.addRecent(q, this.mode);
        this.runSearch();
      }
    });
  }

  /** Close the dropdown on any click outside the search field. */
  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as HTMLElement | null;
    if (!target) return;
    if (this.showRecentSearches && !target.closest('.search-relative')) {
      this.showRecentSearches = false;
    }
    if (this.showModeMenu && !target.closest('.mode-select-wrapper')) {
      this.showModeMenu = false;
    }
  }

  /** Escape closes whichever menu is open. */
  @HostListener('document:keydown.escape')
  onEscape(): void {
    if (this.showModeMenu) this.showModeMenu = false;
    if (this.showRecentSearches) {
      this.showRecentSearches = false;
      this.clearBlurTimeout();
    }
  }

  toggleModeMenu(): void {
    this.showModeMenu = !this.showModeMenu;
  }

  closeModeMenu(): void {
    this.showModeMenu = false;
  }

  onModeOptionClick(value: ModeOption): void {
    this.showModeMenu = false;
    this.onModeChange(value);
  }

  /** Label shown on the mode trigger button — current selection. */
  currentModeLabel(): string {
    return this.modes.find(m => m.value === this.mode)?.label ?? '';
  }

  onSearchFocus(): void {
    this.clearBlurTimeout();
    this.recentSearchesList = this.recentSearches.getRecent();
    this.showRecentSearches = this.recentSearchesList.length > 0;
  }

  onSearchBlur(): void {
    // Deferred so a mousedown on a dropdown row wins the race against blur.
    this.recentSearchBlurTimeout = setTimeout(() => {
      this.showRecentSearches = false;
      this.recentSearchBlurTimeout = null;
    }, 150);
  }

  /** Replay a recent search in the mode it was originally run with. */
  applyRecentSearch(recent: RecentSearch): void {
    this.clearBlurTimeout();
    this.query = recent.q;
    this.mode = recent.mode;
    this.showRecentSearches = false;
    this.onSubmit();
  }

  removeRecentSearch(event: MouseEvent, recent: RecentSearch): void {
    // Stop the row's own mousedown from also running the search.
    event.preventDefault();
    event.stopPropagation();
    this.recentSearchesList = this.recentSearches.removeRecent(recent.q, recent.mode);
    if (this.recentSearchesList.length === 0) {
      this.showRecentSearches = false;
    }
  }

  modeLabel(mode: SearchMode): string {
    const tab = this.modes.find(m => m.value === mode);
    return tab?.label ?? 'Keyword';
  }

  clearRecentSearches(event: MouseEvent): void {
    event.preventDefault();
    event.stopPropagation();
    this.recentSearches.clearRecent();
    this.recentSearchesList = [];
    this.showRecentSearches = false;
  }

  private clearBlurTimeout(): void {
    if (this.recentSearchBlurTimeout) {
      clearTimeout(this.recentSearchBlurTimeout);
      this.recentSearchBlurTimeout = null;
    }
  }

  onQueryChange(value: string): void {
    this.query = value;
  }

  onSubmit(): void {
    const q = this.query.trim();
    if (q) {
      this.recentSearchesList = this.recentSearches.addRecent(q, this.mode);
    }
    this.showRecentSearches = false;
    this.runSearch();
  }

  onSourceChange(value: ExchangeSource): void {
    if (this.source === value) return;
    this.source = value;
    this.runSearch();
  }

  getExchangeValue(key: keyof typeof ExchangeSource): ExchangeSource {
    return ExchangeSource[key];
  }

  onSortChange(value: SortOption): void {
    if (this.sort === value) return;
    this.sort = value;
    this.runSearch();
  }

  onModeChange(value: ModeOption): void {
    if (this.mode === value) return;
    this.mode = value;
    // Only flip UI state; the user has to hit Search to run with the new mode.
    this.semanticError = false;
    this.error = false;
  }

  onResultClick(result: SearchResult): void {
    this.router.navigate(['/circular', result.id]);
  }

  exchangeLabel = exchangeLabel;

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

  safeTitle(title: string, highlights: Record<string, string[]> | null | undefined): SafeHtml {
    // Title-mode searches request highlights on the `title` field with
    // number_of_fragments=0, so the whole title comes back wrapped in
    // <mark>...</mark>. Use it when present, fall back to the raw title.
    const highlighted = highlights?.['title']?.[0];
    if (highlighted) {
      return this.sanitizer.bypassSecurityTrustHtml(highlighted);
    }
    return this.sanitizer.bypassSecurityTrustHtml(title ?? '');
  }

  safeMarkdown(text: string | null | undefined): SafeHtml {
    if (!text) return '';
    const html = marked.parse(text, { async: false }) as string;
    return this.sanitizer.bypassSecurityTrustHtml(html);
  }

  get isKeywordMode(): boolean {
    // Keyword / Exact / Phrase all hit the bm25v2 endpoint today.
    return this.mode !== 'semantic';
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
      // `title` and `phrase` are wired through to the backend's match_mode;
      // `keyword` is the default (sent as undefined).
      matchMode:
        this.mode === 'title' ? 'title'
        : this.mode === 'phrase' ? 'phrase'
        : undefined,
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
