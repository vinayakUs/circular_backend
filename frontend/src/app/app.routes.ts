import { Routes } from '@angular/router';
import { HomeComponent } from './home/home.component';
import { AllCircularsComponent } from './all-circulars/all-circulars.component';
import { CircularDetailComponent } from './circular-detail/circular-detail.component';
import { ChangesComponent } from './changes/changes.component';
import { LoginComponent } from './login/login.component';
import { ExpertsByDepartmentComponent } from './experts-by-department/experts-by-department.component';
import { AlertsComponent } from './alerts/alerts.component';

export const routes: Routes = [
  { path: '', component: HomeComponent },
  { path: 'all-circulars', component: AllCircularsComponent },
  { path: 'changes', component: ChangesComponent },
  { path: 'circular/:id', component: CircularDetailComponent },
  { path: 'login', component: LoginComponent },
  { path: 'experts-by-department', component: ExpertsByDepartmentComponent },
  { path: 'alerts', component: AlertsComponent },
  { path: '**', redirectTo: '' }
];
