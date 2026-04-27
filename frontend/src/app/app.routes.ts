import { Routes } from '@angular/router';
import { HomeComponent } from './home/home.component';
import { AllCircularsComponent } from './all-circulars/all-circulars.component';
import { CircularDetailComponent } from './circular-detail/circular-detail.component';
import { ChangesComponent } from './changes/changes.component';

export const routes: Routes = [
  { path: '', component: HomeComponent },
  { path: 'all-circulars', component: AllCircularsComponent },
  { path: 'changes', component: ChangesComponent },
  { path: 'circular/:id', component: CircularDetailComponent },
  { path: '**', redirectTo: '' }
];
