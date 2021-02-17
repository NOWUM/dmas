import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ConfigAgentsComponent } from './config-agents.component';

describe('ConfigAgentsComponent', () => {
  let component: ConfigAgentsComponent;
  let fixture: ComponentFixture<ConfigAgentsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ConfigAgentsComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigAgentsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
