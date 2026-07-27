import { HttpInterceptorFn, HttpErrorResponse } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';

export const authInterceptor: HttpInterceptorFn = (req, next) => {
  // Extract token from cookie
  const cookies = document.cookie.split(";");
  let token: string | null = null;
  for (const cookie of cookies) {
    const parts = cookie.split('=');
    if (parts.length >= 2) {
      const name = parts[0].trim();
      if (name === 'access_token') {
        token = decodeURIComponent(parts.slice(1).join('='));
        break;
      }
    }
  }

  if (token) {
    // Clone request with Authorization header
    const authReq = req.clone({
      setHeaders: {
        Authorization: `Bearer ${token}`
      }
    });
    return next(authReq);
  }

  // No token found, pass request through without auth header
  // Backend will return 401 which will be handled as appropriate
  return next(req);
};