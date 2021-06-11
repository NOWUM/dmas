/* grid.component.ts
|   Info: Grid-Komponente
|   Typ: TS Logic & HTML Code
|   Inhalt: Bindet das Grid per iframe ein
|   TODO: Umsetzung in Angular, statt fertigen Plot nur über iframe einbinden?
*/
import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-grid',
  template: `
    <iframe src="http://149.201.88.75:5010/Grid" title="Grid from Backend" width="800" height="1000"></iframe>
    <p>grid works!</p>
  `,
  styles: [
  ]
})
export class GridComponent implements OnInit {

  constructor() {
  }
  ngOnInit(): void {
  }

}
