import { TestBed } from '@angular/core/testing';
import { HttpClient, provideHttpClient, withInterceptors } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { of } from 'rxjs';

import { authInterceptor } from './auth.interceptor';
import { AuthService, SessionResponse } from './services/auth.service';
import { LogoutService } from './services/logout.service';

/** Tests for the HttpOnly-cookie auth interceptor. AuthService is stubbed
 * because the SPA cannot seed the cookies these specs would otherwise need. */
describe('authInterceptor', () => {
  let http: HttpClient;
  let httpMock: HttpTestingController;
  let logoutSpy: jasmine.Spy;

  let authenticated: boolean;
  let csrf: string | null;
  let refreshResult: SessionResponse | null;
  let refreshCalls: number;

  const SESSION: SessionResponse = {
    authenticated: true,
    refreshed: true,
    access_expires_at: new Date(Date.now() + 900_000).toISOString(),
    csrf_token: 'csrf-new',
    user: { username: 'alice' },
  };

  const authStub = {
    get csrf() { return csrf; },
    get user() { return authenticated ? SESSION.user : null; },
    isAuthenticated: () => authenticated,
    refreshOnce: () => {
      refreshCalls += 1;
      // The real service adopts the refreshed envelope; mimic that so the
      // replay observes the rotated token.
      if (refreshResult) csrf = SESSION.csrf_token;
      return of(refreshResult);
    },
  };

  beforeEach(() => {
    authenticated = true;
    csrf = 'csrf-old';
    refreshResult = SESSION;
    refreshCalls = 0;

    TestBed.configureTestingModule({
      providers: [
        provideHttpClient(withInterceptors([authInterceptor])),
        provideHttpClientTesting(),
        { provide: AuthService, useValue: authStub },
        { provide: LogoutService, useValue: { logout: jasmine.createSpy('logout') } },
      ],
    });

    http = TestBed.inject(HttpClient);
    httpMock = TestBed.inject(HttpTestingController);
    logoutSpy = TestBed.inject(LogoutService).logout as jasmine.Spy;
  });

  afterEach(() => httpMock.verify());

  describe('outgoing requests', () => {
    it('sends cookies', () => {
      http.get('/api/thing').subscribe();
      const req = httpMock.expectOne('/api/thing');
      expect(req.request.withCredentials).toBeTrue();
      req.flush({});
    });

    it('attaches X-CSRF-Token to unsafe methods', () => {
      http.post('/api/thing', {}).subscribe();
      const req = httpMock.expectOne('/api/thing');
      expect(req.request.headers.get('X-CSRF-Token')).toBe('csrf-old');
      req.flush({});
    });

    it('omits X-CSRF-Token on safe methods', () => {
      http.get('/api/thing').subscribe();
      const req = httpMock.expectOne('/api/thing');
      expect(req.request.headers.has('X-CSRF-Token')).toBeFalse();
      req.flush({});
    });

    it('sends no Authorization header', () => {
      http.get('/api/thing').subscribe();
      const req = httpMock.expectOne('/api/thing');
      expect(req.request.headers.has('Authorization')).toBeFalse();
      req.flush({});
    });
  });

  describe('401 handling', () => {
    it('retries once after refreshing, then succeeds', (done) => {
      http.get('/api/thing').subscribe({ next: () => done() });

      httpMock.expectOne('/api/thing').flush(null, { status: 401, statusText: 'Unauthorized' });
      expect(refreshCalls).toBe(1);

      const retried = httpMock.expectOne('/api/thing');
      expect(retried.request.withCredentials).toBeTrue();
      retried.flush({ ok: true });
    });

    it('replays with the CSRF token the refresh rotated in', (done) => {
      http.post('/api/thing', {}).subscribe({ next: () => done() });

      const first = httpMock.expectOne('/api/thing');
      expect(first.request.headers.get('X-CSRF-Token')).toBe('csrf-old');
      first.flush(null, { status: 401, statusText: 'Unauthorized' });

      const retried = httpMock.expectOne('/api/thing');
      // The stale token here would 403 every unsafe request right after
      // a successful refresh.
      expect(retried.request.headers.get('X-CSRF-Token')).toBe('csrf-new');
      retried.flush({});
    });

    it('logs out when the refresh also reports no session', (done) => {
      refreshResult = null;
      http.get('/api/thing').subscribe({
        error: () => {
          expect(logoutSpy).toHaveBeenCalledWith('/login');
          done();
        },
      });
      httpMock.expectOne('/api/thing').flush(null, { status: 401, statusText: 'Unauthorized' });
    });

    it('does not refresh for 401s from the auth flow', (done) => {
      http.post('/api/auth/login', {}).subscribe({
        error: () => {
          expect(refreshCalls).toBe(0);
          expect(logoutSpy).not.toHaveBeenCalled();
          done();
        },
      });
      httpMock.expectOne('/api/auth/login').flush(null, { status: 401, statusText: 'Unauthorized' });
    });

    it('does not retry /api/auth/session itself', (done) => {
      http.get('/api/auth/session').subscribe({
        error: () => {
          expect(refreshCalls).toBe(0);
          done();
        },
      });
      httpMock.expectOne('/api/auth/session').flush(null, { status: 401, statusText: 'Unauthorized' });
    });

    it('does not refresh when nobody is signed in', (done) => {
      authenticated = false;
      http.get('/api/thing').subscribe({
        error: () => {
          expect(refreshCalls).toBe(0);
          expect(logoutSpy).not.toHaveBeenCalled();
          done();
        },
      });
      httpMock.expectOne('/api/thing').flush(null, { status: 401, statusText: 'Unauthorized' });
    });
  });
});
