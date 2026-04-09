import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import {  IndexService, SearchIndexItem, SearchIndexRespose } from '../../services/index.service';

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

  constructor(private indexService: IndexService) { }

  selectExchange(exchange: string): void {
    this.selectedExchange = exchange;
  }

  public search(): void {
        console.log('result');

    this.indexService.getSearchResult(this.searchQuery).subscribe({
      next: (result:SearchIndexRespose) => {
        this.searchResults =  result.results;
        console.log('result',result);
      }
    });

    

  }



}
