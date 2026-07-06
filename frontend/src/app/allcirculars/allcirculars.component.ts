import { Component, inject, OnInit, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormControl, FormsModule } from '@angular/forms';
import { NavbarComponent } from "../navbar/navbar.component";
import { CircularsApiService, Circular, SemanticSearchResponse, SearchResult, SearchResponse, CircularDbLookupItem } from '../services/circulars-api.service';
import { CircularfilterstateService } from '../services/circularfilterstate.service';
import { RecentSearchesService } from '../services/recent-searches.service';
import { Router, RouterModule } from '@angular/router';
import { debounceTime, distinctUntilChanged, EMPTY, finalize, switchMap, tap } from 'rxjs';
import { ReactiveFormsModule } from '@angular/forms';

export enum ExchangeSource {
  SEBI = 'SEBI',
  NSE = 'NSE',
  ALL = 'ALL',
}

@Component({
  selector: 'app-allcirculars',
  imports: [NavbarComponent, CommonModule, FormsModule, ReactiveFormsModule, RouterModule],
  templateUrl: './allcirculars.component.html',
  styleUrl: './allcirculars.component.css',
  standalone: true
})
export class AllCircularsComponent implements OnInit {

  private apiService = inject(CircularsApiService);
  state = inject(CircularfilterstateService);
  private recentSearches = inject(RecentSearchesService);
  private router = inject(Router);

  loading = true;
  isApiError = false;
  showSignatoryDropdown = false;
  showCircNoDropdown = false;
  circNoDropdownLoading = false;
  // circularNoSelected: string[] = [];
  signatoryOptions: string[] = [];
  exchanges = Object.values(ExchangeSource);
  isSearching = false;
  circulars: Circular[] = [];
  circularNoOptions: CircularDbLookupItem[] = [];
  total = 0;

  // Recent searches for the top search field
  showRecentSearches = false;
  recentSearchesList: string[] = [];
  private recentSearchBlurTimeout: ReturnType<typeof setTimeout> | null = null;


  circNoSearchControl = new FormControl('');

  availableYears: number[] = Array.from({ length: 10 }, (_, i) => new Date().getFullYear() - i);


  pagination = {
    limit: 10,
    offset: 0
  };
  pageInput = '';

  get currentPage(): number {
    if (this.total <= 0) return 1;
    return Math.floor(this.pagination.offset / this.pagination.limit) + 1;
  }

  get totalPages(): number {
    if (this.total <= 0) return 1;
    return Math.max(1, Math.ceil(this.total / this.pagination.limit));
  }

  get canGoPrev(): boolean {
    return this.pagination.offset > 0;
  }

  get canGoNext(): boolean {
    return this.pagination.offset + this.pagination.limit < this.total;
  }

  getAvailableYears(): number[] {
  const currentYear = new Date().getFullYear();
  return Array.from({ length: 4 }, (_, i) => currentYear - i);
}

  ngOnInit(): void {
    console.log('Initial filters:', this.state.filters);
    this.availableYears = this.getAvailableYears();
    this.recentSearchesList = this.recentSearches.getRecent();
    this.loadSignatoryOptions();
    this.loadCirculars();

    this.circNoSearchControl.valueChanges.pipe(
      debounceTime(500),
      distinctUntilChanged(),
      switchMap(value => {
        const searchText = (value ?? '').toString().trim();

        if (!searchText) {
          this.circularNoOptions = [];
          this.circNoDropdownLoading = false;
          return EMPTY;
        }

        this.circNoDropdownLoading = true;
        return this.apiService.getlookupCirculars({ q: searchText, field: 'full_reference' })
          .pipe(
            finalize(() => {
              this.circNoDropdownLoading = false;
            })
          );
      })
    ).subscribe({
      next: (data) => {
        this.circularNoOptions = data.matches;
        console.log('Search results:', this.circularNoOptions);
      },
      error: (error) => {
        console.error('Error during search:', error);
      }
    });


  }
  loadCirculars() {
    // if (this.loading) return;
    this.loading = true;
    this.apiService.getCircularsv2({
      source: this.state.filters.source,
      limit: this.pagination.limit,
      offset: this.pagination.offset,
      from_date: this.state.filters.from_date || undefined,
      to_date: this.state.filters.to_date || undefined,
      search: this.state.filters.search || undefined,
      applicable_to_nse: this.state.filters.applicable_to_nse,
      signatory: this.state.filters.signatory || undefined,
      circular_nos: this.state.filters.circular_nos ? this.state.filters.circular_nos : undefined
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

  // @HostListener('document:click', ['$event'])
  // onDocumentClick(event: MouseEvent): void {
  //   if (!this.showSignatoryDropdown) return;
  //   const target = event.target as HTMLElement | null;
  //   if (target && !target.closest('.multi-select')) {
  //     this.showSignatoryDropdown = false;
  //   }
  // }
  @HostListener('document:click', ['$event'])
  onDocumentClick(event: MouseEvent): void {
    const target = event.target as HTMLElement | null;
    if (!target) return;

    if (this.showSignatoryDropdown && !target.closest('.multi-select')) {
      this.showSignatoryDropdown = false;
    }


    if (this.showCircNoDropdown && !target.closest('.multi-select')) {
      this.showCircNoDropdown = false;
    }

    if (this.showRecentSearches && !target.closest('.search-relative')) {
      this.showRecentSearches = false;
    }
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

  toggleSignatoryDropdown(): void {
    if (!this.showSignatoryDropdown) {
      this.showCircNoDropdown = false;
    }
    this.showSignatoryDropdown = !this.showSignatoryDropdown;
  }

  openCircNoDropdown(): void {
    if (!this.showCircNoDropdown) {
      this.showSignatoryDropdown = false;
    }
    this.showCircNoDropdown = true;
  }

  closeSignatoryDropdown(): void {
    this.showSignatoryDropdown = false;
  }

  isSignatorySelected(name: string): boolean {
    return this.state.filters.signatory.includes(name);
  }

  toggleSignatory(name: string, checked: boolean): void {
    if (checked) {
      if (!this.state.filters.signatory.includes(name)) {
        this.state.filters.signatory = [...this.state.filters.signatory, name];
      }
    } else {
      this.state.filters.signatory = this.state.filters.signatory.filter(s => s !== name);
    }
  }

  onApplicableToNseChange(value: boolean | null): void {
    this.state.filters.applicable_to_nse = value;
    this.onApply();
  }

  onExchangeChange(value: ExchangeSource): void {
    this.state.filters.source = value;
    this.onApply();
  }

  onApply(): void {
    console.log('Applied filters:', this.state.filters);
    this.pagination.offset = 0;
    this.pageInput = '';
    this.loadCirculars();

  }

  onPrevPage(): void {
    if (!this.canGoPrev) return;
    this.pagination.offset = Math.max(0, this.pagination.offset - this.pagination.limit);
    this.pageInput = '';
    this.loadCirculars();
  }

  onNextPage(): void {
    if (!this.canGoNext) return;
    this.pagination.offset += this.pagination.limit;
    this.pageInput = '';
    this.loadCirculars();
  }

  onGoToPage(): void {
    const raw = Number(this.pageInput);
    if (!Number.isFinite(raw) || raw < 1) {
      this.pageInput = '';
      return;
    }
    const target = Math.min(this.totalPages, Math.max(1, Math.floor(raw)));
    this.pagination.offset = (target - 1) * this.pagination.limit;
    this.pageInput = '';
    this.loadCirculars();
  }
  navigateToCircular(circularId: string, circ: any): void {
    console.log("Navigating to circular:", circularId, circ);
    this.router.navigate(['/circular', circularId]);
  }
  clearFilters(): void {
    this.state.filters.selectedyear = null;
    this.state.filters.source = ExchangeSource.SEBI;
    this.state.filters.from_date = '';
    this.state.filters.to_date = '';
    this.state.filters.search = '';
    this.state.filters.signatory = [];
    this.state.filters.applicable_to_nse = null;
    this.pagination.offset = 0;
    this.state.filters.circular_nos = [];   // <-- add this
    this.circNoSearchControl.reset()
    this.loadCirculars();
  }

  onSearch() {
    console.log('Search initiated with query:', this.state.filters.search);

    const query = (this.state.filters.search || '').trim();
    if (!query) return;

    this.recentSearchesList = this.recentSearches.addRecent(query);
    this.showRecentSearches = false;
    this.router.navigate(['/keyword-search'], { queryParams: { q: query, mode: 'keyword' } });
  }

  searchInMode(mode: 'keyword' | 'semantic'): void {
    const query = (this.state.filters.search || '').trim();
    if (!query) return;

    if (this.recentSearchBlurTimeout) {
      clearTimeout(this.recentSearchBlurTimeout);
      this.recentSearchBlurTimeout = null;
    }
    this.recentSearchesList = this.recentSearches.addRecent(query);
    this.showRecentSearches = false;
    this.router.navigate(['/keyword-search'], { queryParams: { q: query, mode } });
  }

  get hasQueryInput(): boolean {
    return (this.state.filters.search || '').toString().trim().length > 0;
  }

  onSearchFocus(): void {
    if (this.recentSearchBlurTimeout) {
      clearTimeout(this.recentSearchBlurTimeout);
      this.recentSearchBlurTimeout = null;
    }
    this.refreshRecentSearches();
    if (this.hasQueryInput) {
      // While the user has typed text, surface the mode-suggestion chips.
      this.showRecentSearches = true;
      return;
    }
    this.showRecentSearches = this.recentSearchesList.length > 0;
  }

  onSearchBlur(): void {
    // Delay so click handlers on dropdown items can fire first.
    this.recentSearchBlurTimeout = setTimeout(() => {
      this.showRecentSearches = false;
      this.recentSearchBlurTimeout = null;
    }, 150);
  }

  onSearchInput(): void {
    // While the user is typing, swap from "recent searches" to the
    // "search using keyword / semantic" suggestions.
    if (this.hasQueryInput) {
      this.showRecentSearches = true;
    } else {
      this.showRecentSearches = this.recentSearchesList.length > 0;
    }
  }

  applyRecentSearch(query: string): void {
    if (this.recentSearchBlurTimeout) {
      clearTimeout(this.recentSearchBlurTimeout);
      this.recentSearchBlurTimeout = null;
    }
    this.state.filters.search = query;
    this.showRecentSearches = false;
    this.onSearch();
  }

  removeRecentSearch(event: MouseEvent, query: string): void {
    event.stopPropagation();
    this.recentSearchesList = this.recentSearches.removeRecent(query);
    if (this.recentSearchesList.length === 0) {
      this.showRecentSearches = false;
    }
  }

  clearRecentSearches(event: MouseEvent): void {
    event.stopPropagation();
    this.recentSearches.clearRecent();
    this.recentSearchesList = [];
    this.showRecentSearches = false;
  }

  private refreshRecentSearches(): void {
    this.recentSearchesList = this.recentSearches.getRecent();
  }

  isCircularNoSelected(id: string): boolean {
    return this.state.filters.circular_nos.includes(id);
  }

  toggleCircularNo(id: string, checked: boolean): void {
    if (checked) {
      if (!this.state.filters.circular_nos.includes(id)) {
        this.state.filters.circular_nos = [...this.state.filters.circular_nos, id];
      }
    } else {
      this.state.filters.circular_nos = this.state.filters.circular_nos.filter(v => v !== id);
    }
  }


  onYearSelect(year: number|null): void {
    this.state.filters.selectedyear = year;
    if(year !== null) {
      this.state.filters.from_date = `${year}-01-01`;
      this.state.filters.to_date = `${year}-12-31`;
    }else{
      this.state.filters.from_date = '';
      this.state.filters.to_date = '';
    }
    this.onApply();
  }
}
