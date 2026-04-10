import { ApplicationConfig } from '@angular/core';
import { provideHttpClient } from '@angular/common/http';
import { provideRouter } from '@angular/router';
import { provideAnimations } from '@angular/platform-browser/animations';
import { routes } from './app.routes';
import { provideToastr } from 'ngx-toastr';

export const appConfig: ApplicationConfig = {
  providers: [provideHttpClient(), 
    provideToastr()  , // global toastr config
    provideRouter(routes),
    provideToastr(), 
    provideAnimations() 
  
  ]
};
