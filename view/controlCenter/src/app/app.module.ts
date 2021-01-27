import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { HttpClientModule } from '@angular/common/http';
import { FormsModule } from '@angular/forms';

import { AppComponent } from './app.component';
import { ConfigComponent } from './config/config.component';
import { InfoComponent } from './info/info.component';
import {AppRoutingModule,routingComponents} from "./app-routing.module";
import {ConfigService} from "./config.service";
import { ConfigAgentsComponent } from './config-agents/config-agents.component';
import { AgentsInfoComponent } from './agents-info/agents-info.component';


@NgModule({
  declarations: [
    AppComponent,
    //ConfigComponent,
    ConfigAgentsComponent,
    InfoComponent,
    //AgentsInfoComponent,
    routingComponents
  ],
  imports: [
    BrowserModule,
    HttpClientModule,
    FormsModule,
    AppRoutingModule
  ],
  providers: [
    //register Services for Dependency Injection
    ConfigService
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
