import { Component } from '@angular/core';
import {HttpClient} from '@angular/common/http';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent {
  title = 'controlCenter';
  serviceTypes: string[] = ['services'];
  agentTypes: string[] = ['PWP', 'RES', 'DEM', 'STR', 'MRK', 'NET'];
  view: string = 'config';
  start: string = '2018-01-01';
  end: string = '2018-12-31';
  readonly ROOT_URL = 'http://149.201.88.75:5010';

  constructor(private http: HttpClient) {
  }

  change_view(type: string): void {
    this.view = type;
  }

  start_simulation(){
    let jsonObject: any = {};
    jsonObject['start'] = this.start;
    jsonObject['end'] = this.end;

    console.log(jsonObject);
    const body = JSON.parse(JSON.stringify(jsonObject));
    const headers = {'content-type': 'application/json'};

    this.http.post(this.ROOT_URL + '/start_simulation', body, {'headers': headers}).subscribe(data => {
      console.log(JSON.stringify(data));
    })

  }

}
