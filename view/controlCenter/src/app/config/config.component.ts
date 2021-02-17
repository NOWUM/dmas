import {Component, OnInit, EventEmitter, Input, Output} from '@angular/core';
//import {HttpClient} from '@angular/common/http';
import {ConfigService} from "../config.service";

@Component({
  selector: 'app-config',
  templateUrl: './config.component.html',
  styleUrls: ['./config.component.css']
})
export class ConfigComponent implements OnInit {
  //@Input() type: string;

  title = 'controlCenter';
  serviceTypes: string[] = ['Control Service'];
  agentTypes: string[] = ['PWP', 'RES', 'DEM', 'STR', 'MRK', 'NET'];
  //view: string = 'config';

  // test: string = 'test';
  // @Input() type: string;
  // @Output() info: EventEmitter<string>;
  config: Map<string, string>;
  number: number;
  start: string = '2018-01-01';
  end: string = '2018-12-31';
  type: string;

  constructor(public service: ConfigService) {
    //TODO: seperate Component for Services similar to ConfigAgentsComponent?
    this.type = 'services';
    this.config = new Map<string, string>();
    this.number = 0;

    // service.type = 'services';
    // service.config = new Map<string, string>();
    // //this.info = new EventEmitter<string>();
    // service.number = 0


    //this.service.get_config();

    // //Orig
    // this.service.ngOnInit();
    // this.config = service.config;

    //service.get_info()
  }

  ngOnInit(): void {
    console.log('OnInit: ' + this.type);
    // this.type = 'services';
    this.config = new Map<string, string>();

    // this.service.get_config('services');
    // this.config = this.service.config;

    //this.config = this.service.get_config(this.config,'services');//klappte
    this.get_config();
  }

  //this.service.get_info();

  // change_view(type: string): void {
  //   this.view = type;
  // }

  // update internal config map
  update_config(key: string, value: string): void {
    this.config.set(key, value);
    console.log(this.config);
  }

  get_config(){
    this.service.get_config(this.type)
      // // clone the data object, using its known Config shape
      // // ERROR TypeError: this.config.set is not a function
      // // at ConfigComponent.update_config (config.component.ts:67)
      // .subscribe((data:Map<string, string>) => this.config = { ...data });//klappt fürs Auslesen, Error s. oben

      // alte Variante von Rieke (klappt auch):
      .subscribe((data: any) => {
        console.log('get_config(): Subscription received for ' + this.type + ':');
        console.log(data);
        for (var value in data) {
          this.config.set(value, data[value]);
        }
      });
  }

  // get_config(){
  // this.service.get_config(this.type)
  //   // clone the data object, using its known Config shape
  //   .subscribe((data:Map<string, string>) => this.config = { ...data });
  // }

}
