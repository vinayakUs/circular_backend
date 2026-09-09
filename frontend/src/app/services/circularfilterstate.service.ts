import { Injectable } from '@angular/core';
import { ExchangeSource } from '../util/source.util';

@Injectable({
  providedIn: 'root'
})
export class CircularfilterstateService {

  filters = {
    source: ExchangeSource.SEBI,
    from_date: '',
    to_date: '',
    applicable_to_nse: null as boolean | null,
    signatory: [] as string[],
    circular_nos: [] as string[],
    selectedyear: null as number | null,
    department: '' as string,
  };
}
