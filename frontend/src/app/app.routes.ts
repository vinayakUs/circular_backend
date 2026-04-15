import { Routes } from '@angular/router';
import { HomepageComponent } from './components/homepage/homepage.component';
import { SearchpageComponent } from './components/searchpage/searchpage.component';
import { CircularViewComponent } from './components/circular-view/circular-view.component';

export const routes: Routes = [
  {
    path: '',
    component: SearchpageComponent,
    title: 'Search Page'
  },
  {
    path: 'home',
    component: HomepageComponent,
    title: 'Home'
  },
  {
    path: 'circulars/:id',
    component: CircularViewComponent,
    title: 'Circular View'
  }
];
