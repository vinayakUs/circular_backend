import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { LoginService } from './services/login.service';

export const authGuard: CanActivateFn = (route, state) => {
  const auth = inject(LoginService);
    return auth.isAuthenticated() || inject(Router).createUrlTree(['/login']);  
};
