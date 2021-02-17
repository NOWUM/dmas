import {Component, Input, OnInit} from '@angular/core';
import {ConfigService} from "../config.service";

@Component({
  selector: 'app-config-agents',
  templateUrl: './config-agents.component.html',
  styleUrls: ['./config-agents.component.css']
})
export class ConfigAgentsComponent implements OnInit {

  @Input() type: string;
  agentTypes: string[] = ['PWP', 'RES', 'DEM', 'STR', 'MRK', 'NET'];
  config: Map<string, string>;
  //number: Promise<number>;
  number: number;

  constructor(public service: ConfigService) {
    //Wird nur einmal aufgerufen?
    this.type = "agentX";
    //this.type = 'services';
    this.config = new Map<string, string>();
    this.number = 0;
    //this.info = new EventEmitter<string>();
    // service.type = 'services';
    // service.number = 0;
    //console.log(this.type);//gibt an dieser stelle 6x agentX aus
  }

  ngOnInit(): void {
    console.log('OnInit: ' + this.type);
    // hier agent spezifische befehele aufrufen
    // this.service.type = this.type;
    // this.service.config = new Map<string, string>();
    // this.service.type = this.type;
    // this.service.ngOnInit();
    //this.number = 11;
    this.config = new Map<string, string>();

    //console.log('OnInit: ' + this.type);
    //this.service.type = this.type;
    //this.service.ngOnInit();

    // this.service.get_config(this.type);
    // this.config = this.service.config; //config enhält auch noch Werte von services (database etc.) WARUM??? wahrscheinlich weil Singelton -> gelöst mit neuer get_config()

    //this.config = this.service.get_config(this.config, this.type);//klappt
    //let testConfig = new Map<string, string>();
    //this.config = this.service.get_config(testConfig, this.type);//klappte

    //this.config = this.service.get_config(this.type);//klappte
    this.get_config();

    //this.service.get_running_agents(this.type).subscribe((data:number) => this.number = data); //https://angular.io/guide/http
    this.get_running_agents();


  }

  // update internal config map
  update_config(key: string, value: string): void {
    this.config.set(key, value);
    console.log(this.config);
  }

  get_config(){
    this.service.get_config(this.type)
      // clone the data object, using its known Config shape
      //.subscribe((data:Map<string, string>) => this.config = { ...data });//auslesen klappt, aber Error beim Ändern der Inputs: ERROR TypeError: this.config.set is not a function -> at ConfigAgentsComponent.update_config (config-agents.component.ts:61)

      // alte Variante von Rieke (klappt):
      .subscribe((data: any) => {
      //.subscribe((data: Map<string, string>) => {//Angabe des Datentyps klappt hier nicht, WARUM?

        console.log('get_config(): Subscription received for ' + this.type + ':');
        console.log(data);
        for (var value in data) {
          this.config.set(value, data[value]);
        }
      });
  }

  // get number of running agents per typ
  get_running_agents(){
    console.log('get_running_agents(): subscribing for ' + this.type);
    // //this.number = this.service.get_running_agents(this.type);
    // this.service.get_running_agents(this.type);
    // console.log('get_running_agents:' + this.number);
    this.service.get_running_agents(this.type).subscribe((data:number) => {
      this.number = data;
      console.log('get_running_agents(): Subscription received for ' + this.type + ': ' + this.number);
    }); //https://angular.io/guide/http
  }

}
