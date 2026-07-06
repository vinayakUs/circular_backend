import { Routes } from '@angular/router';
import { HomeComponent } from './home/home.component';
import { AllCircularsComponent } from './allcirculars/allcirculars.component';
import { CircularDetailComponent } from './circular-detail/circular-detail.component';
import { ChangesComponent } from './changes/changes.component';
import { LoginComponent } from './login/login.component';
import { ExpertsByDepartmentComponent } from './experts-by-department/experts-by-department.component';
import { AlertsComponent } from './alerts/alerts.component';
import { KeywordSearchComponent } from './keyword-search/keyword-search.component';
import { PdfViewerPageComponent } from './pdf-viewer-page/pdf-viewer-page.component';
import { TaskviewComponent } from './taskview/taskview.component';
import { authGuard } from './auth.guard';

export const routes: Routes = [
  { path: '', component: HomeComponent },
  { path: 'changes', component: ChangesComponent },
  { path: 'circular/:id', component: CircularDetailComponent },
  { path: 'login', component: LoginComponent },
  { path: 'experts-by-department', component: ExpertsByDepartmentComponent },
  { path: 'alerts', component: AlertsComponent },
  { path: 'keyword-search', component: KeywordSearchComponent },
  { path: 'allcirculars', component: AllCircularsComponent },
  { path: 'pdf-viewer/:id', component: PdfViewerPageComponent },
  { path: 'taskview/:id', component: TaskviewComponent, canActivate: [authGuard] },

  { path: '**', redirectTo: '' }
];
