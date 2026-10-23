import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import {
  ActivatedRouteSnapshot,
  CanActivateFn,
  Router,
  RouterStateSnapshot,
  UrlTree,
  provideRouter,
} from '@angular/router';
import { Observable } from 'rxjs';

import { authGuard, guestOnlyGuard } from './auth.guard';
import { AuthService, SessionResponse } from './services/auth.service';

type GuardResult = boolean | UrlTree | Observable<boolean | UrlTree>;

const AUTHED: SessionResponse = {
  authenticated: true,
  refreshed: false,
  access_expires_at: '',
  csrf_token: 'c',
  user: { username: 'a' },
};

describe('auth guards', () => {
  let router: Router;
  let httpMock: HttpTestingController;
  let service: AuthService;

  const run = (guard: CanActivateFn): GuardResult =>
    TestBed.runInInjectionContext(() =>
      guard({} as ActivatedRouteSnapshot, {} as RouterStateSnapshot),
    ) as GuardResult;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(),
        provideHttpClientTesting(),
        provideRouter([
          { path: 'login', children: [] },
          { path: '', children: [] },
        ]),
      ],
    });
    router = TestBed.inject(Router);
    httpMock = TestBed.inject(HttpTestingController);
    service = TestBed.inject(AuthService);
  });

  afterEach(() => httpMock.verify());

  describe('authGuard', () => {
    it('allows through without HTTP when already authenticated', () => {
      service.adopt(AUTHED);
      expect(run(authGuard)).toBeTrue();
      httpMock.expectNone('/api/auth/session');
    });

    it('redirects to /login without HTTP when already anonymous', () => {
      service.reset();
      expect(router.serializeUrl(run(authGuard) as UrlTree)).toBe('/login');
      httpMock.expectNone('/api/auth/session');
    });

    it('resolves the session when the status is unknown', (done) => {
      expect(service.status()).toBe('unknown');

      (run(authGuard) as Observable<boolean | UrlTree>).subscribe((value) => {
        expect(value).toBeTrue();
        done();
      });
      httpMock.expectOne('/api/auth/session').flush(AUTHED);
    });

    it('redirects to /login when the session resolves anonymous', (done) => {
      (run(authGuard) as Observable<boolean | UrlTree>).subscribe((value) => {
        expect(router.serializeUrl(value as UrlTree)).toBe('/login');
        done();
      });
      httpMock.expectOne('/api/auth/session')
        .flush(null, { status: 401, statusText: 'Unauthorized' });
    });

    it('allows through on a network error rather than logging the user out', (done) => {
      (run(authGuard) as Observable<boolean | UrlTree>).subscribe((value) => {
        expect(value).toBeTrue();
        done();
      });
      httpMock.expectOne('/api/auth/session')
        .flush(null, { status: 500, statusText: 'Server Error' });
    });
  });

  describe('guestOnlyGuard', () => {
    it('sends an authenticated user home', () => {
      service.adopt(AUTHED);
      expect(router.serializeUrl(run(guestOnlyGuard) as UrlTree)).toBe('/');
    });

    it('shows the login form to an anonymous user', () => {
      service.reset();
      expect(run(guestOnlyGuard)).toBeTrue();
    });
  });
});