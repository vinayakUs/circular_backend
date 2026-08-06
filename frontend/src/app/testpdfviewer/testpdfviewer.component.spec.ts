import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TestpdfviewerComponent } from './testpdfviewer.component';

describe('TestpdfviewerComponent', () => {
  let component: TestpdfviewerComponent;
  let fixture: ComponentFixture<TestpdfviewerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TestpdfviewerComponent]
    })
    .compileComponents();

    fixture = TestBed.createComponent(TestpdfviewerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
