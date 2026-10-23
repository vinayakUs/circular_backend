import { APP_INITIALIZER, ApplicationConfig } from '@angular/core';
import { provideRouter, withInMemoryScrolling } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';

import { routes } from './app.routes';
import { authInterceptor } from './auth.interceptor';
import { AuthService } from './services/auth.service';
import { CurrentPersonaService } from './services/current-persona.service';

/**
 * Resolve the session before the first render, so the guards get a decided
 * auth status and components have a csrf token before firing unsafe requests.
 *
 * Failures are swallowed so the status stays 'unknown' — a network error
 * must not look like a logout, and must not block app start.
 */
function bootstrap(auth: AuthService, persona: CurrentPersonaService) {
  return async () => {
    try {
      const session = await firstValueFrom(auth.session());
      if (session) {
        persona.setFromUser(session.user);
      }
    } catch {
      // Swallowed on purpose: see above.
    }
  };
}

export const appConfig: ApplicationConfig = {
  providers: [
    provideRouter(
      routes,
      // Preserve the URL fragment on navigation and let `ActivatedRoute.fragment`
      // emit it — without these, deep-links like
      // /taskview?...#comment-<uuid> lose their hash on first navigation
      // and `route.fragment.subscribe(...)` never fires.
      withInMemoryScrolling({
        anchorScrolling: 'enabled',
        scrollPositionRestoration: 'enabled',
      }),
    ),
    provideHttpClient(withInterceptors([authInterceptor])),

    {
      provide: APP_INITIALIZER,
      useFactory: bootstrap,
      deps: [AuthService, CurrentPersonaService],
      multi: true,
    },
  ],
};
