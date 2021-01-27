import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-grid',
  template: `
    TODO: Implement Grid without Navbar here?
    <div class="row bg-light text-dark" *ngIf="test == 'test'">
      <p>!Hi!</p>
    </div>
<!--    <div *ngFor="let type of agentTypes">-->
<!--        <h6 style="padding-bottom: 2px; padding-top: 5px">{{ type }}-Agent Options</h6>-->
<!--&lt;!&ndash;        <app-config [type]=type (info)="change_view($event)"> </app-config>&ndash;&gt;-->
<!--      </div>-->
    <p>
      grid works!
    </p>

  `,
  styles: [
  ]
})
export class GridComponent implements OnInit {

  constructor() { }
  test: string = 'test';
  //agentTypes: string[] = ['PWP', 'RES', 'DEM', 'STR', 'MRK', 'NET'];
  ngOnInit(): void {
  }

}
