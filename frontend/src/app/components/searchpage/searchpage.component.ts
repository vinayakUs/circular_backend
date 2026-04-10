import { Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {  IndexService, SearchIndexItem, SearchIndexRespose } from '../../services/index.service';
import { ToastrService } from 'ngx-toastr';

@Component({
  selector: 'app-searchpage',
  standalone: true,
  imports: [FormsModule],
  templateUrl: './searchpage.component.html',
  styleUrl: './searchpage.component.css'
})
export class SearchpageComponent {
  searchQuery = 'sebi';
  readonly exchanges = ['All', 'NSE', 'SEBI'];
  selectedExchange = 'All';
  searchResults: SearchIndexItem[] = [];
  isSearching = signal<boolean>(false);
  toastr = inject(ToastrService);

  constructor(private indexService: IndexService) { }

  selectExchange(exchange: string): void {
    this.selectedExchange = exchange;
  }

  public search(): void {
        console.log('result');

    this.isSearching.set(true);

    const reqExchange = this.selectedExchange === 'All' ? this.exchanges.filter(item=> item !== 'All') : [this.selectedExchange];

    this.indexService.getSearchResult(this.searchQuery,reqExchange).subscribe({
      next: (result:SearchIndexRespose) => {
        this.searchResults =  result.results;
        console.log('result',result);
      },
      error: (error) => {
        console.error('Error fetching search results:', error);
        this.toastr.error('Failed to fetch search results. Please try again later.', 'Error');
      },
      complete: () => {
        this.isSearching.set(false);
      }
    });

    

  }



}
