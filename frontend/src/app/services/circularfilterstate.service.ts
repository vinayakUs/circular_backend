import { Injectable } from '@angular/core';
import { ExchangeSource } from '../allcirculars/allcirculars.component';

@Injectable({
  providedIn: 'root'
})
export class CircularfilterstateService {

  filters = {
    source: ExchangeSource.SEBI,
    from_date: '',
    to_date: '',
    search: '',
    applicable_to_nse: null as boolean | null,
    signatory: [] as string[],
    circular_nos: [] as string[],
    selectedyear: null as number | null,
  };
}
