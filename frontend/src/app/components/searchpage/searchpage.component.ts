import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { IndexSearchResult, IndexService } from '../../services/index.service';

@Component({
  selector: 'app-searchpage',
  standalone: true,
  imports: [FormsModule],
  templateUrl: './searchpage.component.html',
  styleUrl: './searchpage.component.css'
})
export class SearchpageComponent {
  searchQuery = 'adf';
  readonly exchanges = ['All', 'NSE', 'SEBI'];
  selectedExchange = 'All';
  searchResults: IndexSearchResult[] = [];

  constructor(private indexService: IndexService) { }

  selectExchange(exchange: string): void {
    this.selectedExchange = exchange;
  }

  public search(): void {
        console.log('result');

    this.indexService.getSearchResult(this.searchQuery).subscribe({
      next: (result:IndexSearchResult[]) => {
        this.searchResults =result;
        console.log(result);
      }
    });

    

  }



}
