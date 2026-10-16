import { TestBed } from '@angular/core/testing';
import {
  HttpClient,
  HttpErrorResponse,
  provideHttpClient,
  withInterceptors,
} from '@angular/common/http';
import {
  HttpTestingController,
  provideHttpClientTesting,
} from '@angular/common/http/testing';
import { provideRouter, Router } from '@angular/router';
import { authInterceptor } from './auth.interceptor';
import { CurrentPersonaService } from './services/current-persona.service';
import { LoginService } from './services/login.service';

/**
 * Tests for the global auth state cleanup behavior the interceptor owns.
 *
 * Production bug being fixed:
 *   When the backend returns 401 (token expired, revoked, cleared in another
 *   tab) the SPA used to keep the previous user's persona visible in the
 *   navbar because no client-side code path besides the manual Logout
 *   button was clearing persona + user_details + token cookie.
 *
 * Expected behavior after the fix:
 *   1. A 401 from any authed, non-auth-flow request clears persona state
 *      (signal + localStorage) and redirects to /login.
 *   2. 401s from /api/auth/login|captcha|mfa/* are NOT treated as logout
 *      signals — the login flow itself depends on them to show validation
 *      errors.
 *   3. Multiple parallel 401s do not cause duplicate navigation or reset.
 */
describe('authInterceptor', () => {
  let http: HttpClient;
  let httpMock: HttpTestingController;
  let router: Router;
  let personaSvc: CurrentPersonaService;
  let navigateSpy: jasmine.Spy;

  /**
   * Seed an access_token cookie so the interceptor thinks the user is
   * authenticated. The interceptor only triggers auto-logout on requests
   * it had authed in the first place.
   */
  function setCookie(value: string) {
    document.cookie = `access_token=${value}; path=/`;
  }
  function clearCookie() {
    document.cookie = 'access_token=; path=/; expires=Thu, 01 Jan 1970';
  }

  beforeEach(() => {
    setCookie('valid.jwt.token');
    localStorage.setItem(
      'user_details',
      JSON.stringify({ username: 'alice', persona_slug: 'compliance', persona: { slug: 'compliance', name: 'Compliance' } }),
    );

    TestBed.configureTestingModule({
      providers: [
        provideRouter([{ path: 'login', children: [] }, { path: '', children: [] }]),
        provideHttpClient(withInterceptors([authInterceptor])),
        provideHttpClientTesting(),
      ],
    });

    http = TestBed.inject(HttpClient);
    httpMock = TestBed.inject(HttpTestingController);
    router = TestBed.inject(Router);
    personaSvc = TestBed.inject(CurrentPersonaService);
    personaSvc.setFromMe({
      persona_slug: 'compliance',
      persona: { slug: 'compliance', name: 'Compliance' } as any,
    });

    navigateSpy = spyOn(router, 'navigate').and.callThrough();
  });

  afterEach(() => {
    httpMock.verify();
    clearCookie();
    localStorage.removeItem('user_details');
    personaSvc.reset();
  });

  it('clears persona + localStorage + redirects to /login on 401 from an authed request', (done) => {
    http.get('/api/some-protected-resource').subscribe({
      next: () => done.fail('expected 401 to propagate as error'),
      error: (err: HttpErrorResponse) => {
        expect(err.status).toBe(401);
        // Persona signal cleared.
        expect(personaSvc.slug()).toBeNull();
        expect(personaSvc.persona()).toBeNull();
        // localStorage cleared.
        expect(localStorage.getItem('user_details')).toBeNull();
        // Cookie cleared.
        expect(document.cookie).not.toContain('access_token=valid.jwt.token');
        // Navigated to /login.
        expect(navigateSpy).toHaveBeenCalledWith(['/login']);
        done();
      },
    });

    const req = httpMock.expectOne('/api/some-protected-resource');
    req.flush('unauthorized', { status: 401, statusText: 'Unauthorized' });
  });

  it('does NOT auto-logout on 401 from /api/auth/login', (done) => {
    http.post('/api/auth/login', { username: 'bob' }).subscribe({
      error: (err: HttpErrorResponse) => {
        expect(err.status).toBe(401);
        // Persona state preserved — login flow handles its own 401.
        expect(personaSvc.slug()).toBe('compliance');
        expect(navigateSpy).not.toHaveBeenCalled();
        done();
      },
    });

    const req = httpMock.expectOne('/api/auth/login');
    req.flush('bad creds', { status: 401, statusText: 'Unauthorized' });
  });

  it('does NOT auto-logout on 401 from /api/auth/mfa/verify', (done) => {
    http.post('/api/auth/mfa/verify', { otp: '000000' }).subscribe({
      error: (err: HttpErrorResponse) => {
        expect(err.status).toBe(401);
        expect(personaSvc.slug()).toBe('compliance');
        expect(navigateSpy).not.toHaveBeenCalled();
        done();
      },
    });

    const req = httpMock.expectOne('/api/auth/mfa/verify');
    req.flush('bad code', { status: 401, statusText: 'Unauthorized' });
  });

  it('does not auto-logout when no token cookie is present (anonymous 401)', (done) => {
    clearCookie(); // simulate expired/cleared session
    http.get('/api/some-public-but-protected-endpoint').subscribe({
      error: (err: HttpErrorResponse) => {
        expect(err.status).toBe(401);
        // Persona signal that was set during the test fixture
        // must NOT be reset just because the request was anonymous.
        // (The interceptor only cleans up when it had authed the request.)
        expect(personaSvc.slug()).toBe('compliance');
        expect(navigateSpy).not.toHaveBeenCalled();
        done();
      },
    });

    const req = httpMock.expectOne('/api/some-public-but-protected-endpoint');
    req.flush('unauthorized', { status: 401, statusText: 'Unauthorized' });
  });

  it('deduplicates concurrent 401s to a single logout+navigate', (done) => {
    let err1 = false, err2 = false;
    http.get('/api/a').subscribe({ error: () => (err1 = true) });
    http.get('/api/b').subscribe({ error: () => (err2 = true) });

    const reqA = httpMock.expectOne('/api/a');
    reqA.flush('nope', { status: 401, statusText: 'Unauthorized' });
    const reqB = httpMock.expectOne('/api/b');
    reqB.flush('nope', { status: 401, statusText: 'Unauthorized' });

    setTimeout(() => {
      expect(err1).toBe(true);
      expect(err2).toBe(true);
      // Exactly one navigation, despite two parallel 401s.
      expect(navigateSpy.calls.allArgs().filter((a) => a[0][0] === 'login').length).toBe(1);
      done();
    }, 0);
  });
});
