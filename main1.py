#!/usr/bin/env python
# -*- coding: cp1252 -*-
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#**MASSA LOMBARDA** DELIBERE DI GIUNTA - 29/12/2012 AGGIORNAMENTO DEI PARAMETRI PER IL CALCOLO DEI CANONI DI EDILIZIA RESIDENZIALE P
import webapp2
import twitter
import oauth2 as oauth
import httplib2
import iri2uri
import datetime
from datetime import date
import time
import logging
import string
try:
    import json                # Python 2.7.
except ImportError:
    import simplejson as json  # Python 2.5.
import facebook
import re
import mechanize
import traceback

from google.appengine.api import app_identity
from google.appengine.api import urlfetch
from google.appengine.ext import db

class PostedEntry(db.Model):
  chiave = db.StringProperty(required=True, indexed = True)
  scadenza = db.StringProperty(required=True, indexed = True)
  fb_messageid = db.StringProperty(required=True, indexed = True)

#contiente le entry che devono ancora essere postate
class AlboEntry(db.Model):
    key_ = db.StringProperty(required=True, indexed = True, multiline=True)
    oggetto = db.StringProperty(required=True, indexed = False, multiline=True)
    datapubbfrom = db.StringProperty(required=True, indexed = True)
    datapubbto = db.StringProperty(required=True, indexed = True)
    url_allegato = db.StringProperty(required=False)
    tipodoc = db.StringProperty(required=True, indexed = True, multiline=True)
    fbmsgid = db.StringProperty(required=False, indexed = True)
    comune = db.StringProperty(required=True, indexed = True)

#contiente le entry che sono state postate   
class PostedKey(db.Model):
    key_ = db.StringProperty(required=True, indexed = True, multiline=True)
    oggetto = db.StringProperty(required=True, indexed = False, multiline=True)
    datapubbfrom = db.StringProperty(required=True, indexed = True)
    datapubbto = db.StringProperty(required=True, indexed = True)
    url_allegato = db.StringProperty(required=False)
    tipodoc = db.StringProperty(required=True, indexed = True, multiline=True)
    fbmsgid = db.StringProperty(required=False, indexed = True)
    comune = db.StringProperty(required=True, indexed = True)

class DataGetter(webapp2.RequestHandler):
    ALBO_ALFONSINE = "albo_alfonsine_1"
    ALBO_COTIGNOLA = "albo_cotignola_1"
    ALBO_CONSELICE = "albo_conselice_1"
    ALBO_BAGNARA = "albo_bagnara_1"
    ALBO_SANTAGATA = "albo_santagata_1"
    ALBO_MASSA = "albo_massa_1"
    ALBO_FUSIGNANO = "albo_fusignano"
    ALBO_BAGNACAVALLO = "albo_bagnacavallo"
    ALBO_LUGO = "albo_lugo_2"
    ALBO_UNIONE = "albo_unione"


    #si aspetta un oggetto di tipo date e retituisce una stringa aaaammgg
    def formatta_date_sortable(self,data):
        mese = data.month
        if mese < 10:
            mese = "0" + str(mese)
        giorno = data.day
        if giorno < 10:
            giorno = "0"+ str(giorno)
        return  str(data.year)+ str(mese) + str(giorno)


    #si aspetta un oggetto di tipo date e retituisce una stringa gg/mm/aaaa
    def formatta_data_italia(self,data):
        mese = data.month
        if mese < 10:
            mese = "0" + str(mese)
        giorno = data.day
        if giorno < 10:
            giorno = "0"+ str(giorno)
        return str(giorno) + "/" + str(mese) + "/" + str(data.year)

    #trasforma una data gg/mm/aaaa in una data aaaammgg
    def formatta_data_sortable(self, data): 
        return data[6:10]+data[3:5]+data[0:2]


    #nelle entry dell'unione cerca il nome di un comune nell'oggetto e lo restiruisce
    def cerca_comune(self,unione_jsonentry):
        oggetto = unione_jsonentry["oggetto"]
        if 'LUGO' in oggetto:
          return 'LUGO'
        elif 'BAGNACAVALLO' in oggetto:
          return 'BAGNACAVALLO'
        elif 'ALFONSINE' in oggetto:
          return 'ALFONSINE'
        elif 'FUSIGNANO' in oggetto:
          return 'FUSIGNANO'
        elif 'AGATA' in oggetto:
          return 'S.AGATA'
        elif 'LOMBARDA' in oggetto:
          return 'MASSA LOMBARDA'
        elif 'CONSELICE' in oggetto:
          return 'CONSELICE'
        elif 'BAGNARA' in oggetto:
          return 'BAGNARA'
        elif 'COTIGNOLA' in oggetto:
          return 'COTIGNOLA'
        else:
          return 'UNIONE'

    def isNone(self,string):
        if string is None:
            return "None"
        else:
            return string

    #interroga scraperwiki per prendere tutte le entry di un certo nomealbo
    def get_albo_entry_list(self,nomealbo):
        h = httplib2.Http()
        today=datetime.date.today()
        interval_day = datetime.timedelta(days=8)
        data_inizio = today - interval_day
        data_inizio_sortable = self.formatta_date_sortable(data_inizio)
        query = "https://api.scraperwiki.com/api/1.0/datastore/sqlite?format=jsondict&name="+nomealbo+"&query=select%20*%20from%20%60swdata%60%20where%20substr(datapubbfrom%2C7%2C4)%20%7C%7C%20substr(datapubbfrom%2C4%2C2)%20%7C%7C%20substr(datapubbfrom%2C1%2C2)%20%3E%20'"+data_inizio_sortable+"'"
        #query = "https://api.scraperwiki.com/api/1.0/datastore/sqlite?format=jsondict&name="+nomealbo+"&query=select%20*%20from%20%60swdata%60"
        resp, content = h.request(query)
        return content

    def crea_chiave(self,entry,comune):
        return self.isNone(entry["key"]+"-"+comune).replace("\n","").replace("\t","").replace("\r","")

    #memorizza nel db di le entry dell'albo prese da Scraperwiki
    def store_entry_list(self,content,comune):
        decoded = json.loads(content)
        if (content <> "[]") :
            for entry in decoded:
                
                try:
                    chiave = self.crea_chiave(entry,comune)
                except Exception:
                    traceback.print_exc()
                    logging.info(entry)
                    continue
                if self.check_stored(chiave) == 0 :
                    self.store_entry(entry, comune)
                else :
                    logging.info("Entry già presente")
        
    def check_stored(self,chiave):
        posted = db.GqlQuery("SELECT * FROM AlboEntry WHERE key_= :1",chiave)
        posted.run()
        for e in posted:
           return 1
        return 0

    def store_entry(self, entry, comune):
       if comune == "UNIONE" :
           ilcomune = self.cerca_comune(entry)
       else:
           ilcomune = comune
       logging.info("Memorizzo in db :" + entry["key"])
       e = AlboEntry(key_=self.crea_chiave(entry,comune), oggetto = self.isNone(entry["oggetto"]), datapubbfrom=self.formatta_data_sortable(self.isNone(entry["datapubbfrom"])), datapubbto=self.formatta_data_sortable(self.isNone(entry["datapubbto"])), url_allegato = self.isNone(entry["URL_allegato"]),tipodoc = self.isNone(entry["tipodoc"]),comune = ilcomune ,fbmsgid="") 
       e.put()

    def get_store_entry_list(self, comune, nomealbo):
        try :
            content = self.get_albo_entry_list(nomealbo)
            self.store_entry_list(content, comune)
        except Exception:
            traceback.print_exc()
            logging.error("Problemi nell'interrogazione dello scraper o nella memorizzazione delle entry")
            return
        
    

    def get(self):
        
        self.get_store_entry_list("UNIONE",self.ALBO_UNIONE)
        self.get_store_entry_list("LUGO",self.ALBO_LUGO)
        self.get_store_entry_list("ALFONSINE",self.ALBO_ALFONSINE)
        self.get_store_entry_list("FUSIGNANO",self.ALBO_FUSIGNANO)
        self.get_store_entry_list("BAGNACAVALLO",self.ALBO_BAGNACAVALLO)
        self.get_store_entry_list("S.AGATA",self.ALBO_SANTAGATA)
        self.get_store_entry_list("MASSA LOMBARDA",self.ALBO_MASSA)
        self.get_store_entry_list("CONSELICE",self.ALBO_CONSELICE)
        self.get_store_entry_list("BAGNARA",self.ALBO_BAGNARA)
        self.get_store_entry_list("COTIGNOLA",self.ALBO_COTIGNOLA)


class MessagePoster(webapp2.RequestHandler):

    MAX_POSTS_PER_RUN = 2
    MAX_AGGREGATE_MSG = 10000
    WHERE_TO_AGGREGATE = "comune= "

    @staticmethod
    def create_short_url(long_url):
        
        scope = "https://www.googleapis.com/auth/urlshortener"
        authorization_token, _ = app_identity.get_access_token(scope)
        logging.info("Using token %s to represent identity %s",
                 authorization_token, app_identity.get_service_account_name())
        
        payload = json.dumps({"longUrl": long_url})
        response = urlfetch.fetch(
            "https://www.googleapis.com/urlshortener/v1/url?pp=1",
            method=urlfetch.POST,
            payload=payload,
            headers = {"Content-Type": "application/json",
                       "Authorization": "OAuth " + authorization_token})
        if response.status_code == 200:
            result = json.loads(response.content)
            return result["id"]
        raise Exception("Call failed. Status code %s. Body %s",
                    response.status_code, response.content)

    def isNone(self,string):
        if string is None:
            return "None"
        else:
            return string

    #si aspetta un oggetto di tipo date e retituisce una stringa gg/mm/aaaa
    def formatta_data_italia(self,data):
        mese = data.month
        if mese < 10:
            mese = "0" + str(mese)
        giorno = data.day
        if giorno < 10:
            giorno = "0"+ str(giorno)
        return str(giorno) + "/" + str(mese) + "/" + str(data.year)

    #si aspetta un oggetto di tipo date e retituisce una stringa aaaammgg
    def formatta_date_sortable(self,data):
        mese = data.month
        if mese < 10:
            mese = "0" + str(mese)
        giorno = data.day
        if giorno < 10:
            giorno = "0"+ str(giorno)
        return  str(data.year)+ str(mese) + str(giorno)

    #si aspetta un oggetto di tipo stringa e retituisce una stringa gg/mm/aaaa
    def formatta_datastring_italia(self,data):
        return data[6:] + "/" + data[4:6] + "/" + data[0:4]

    #trasforma una data gg/mm/aaaa in una data aaaammgg
    def formatta_data_sortable(self, data): 
        return data[6:10]+data[3:5]+data[0:2]

    def isPosted(self,entry):
        entrylist = db.GqlQuery("SELECT * FROM PostedKey WHERE key_=:1",entry.key_)
        for e in entrylist:
            logging.info("Entry già postata")
            return 1
        return 0

    def deletePosted(self):
        entrylist = db.GqlQuery("SELECT * FROM PostedKey ")
        for e in entrylist:
            e.delete()
    
    def get_messages_to_post(self, comune) :
        today=datetime.date.today()
        interval_day = datetime.timedelta(days=7)
        data_inizio = today - interval_day
        data_inizio_sortable = self.formatta_date_sortable(data_inizio)
        logging.info("Prendo i messaggi successivi a " + data_inizio_sortable)
        entrylisttopost = db.GqlQuery("SELECT * FROM AlboEntry WHERE " + self.WHERE_TO_AGGREGATE + ":1 and fbmsgid='' and datapubbfrom > "+data_inizio_sortable+"  order by datapubbfrom desc",comune)
        entrylisttopost.run()
        return entrylisttopost

    def update_db_posted(self, entrylist,fbmsg_id):
        for e in entrylist:
            pk = PostedKey(key_=e.key_, oggetto = e.oggetto, datapubbfrom=e.datapubbfrom, datapubbto=e.datapubbto, url_allegato = e.url_allegato,tipodoc = e.tipodoc,comune = e.comune ,fbmsgid=fbmsg_id) 
            pk.put()
            
    def create_fb_post(self, entrylisttopost):
        msg = ''
        for e in entrylisttopost:
            if self.isPosted(e) == 0 and e.datapubbfrom > "20121230":
                pos = e.key_.find("-")
                prot = e.key_[0:pos]
                msg = msg + "\n**" + e.comune.upper() +"**" + "\n" + e.tipodoc.upper() + " - " + self.formatta_datastring_italia(e.datapubbfrom) + "\n" + e.oggetto + "\n" + "PDF:" + e.url_allegato + "\n" + "Ref:" + prot+ "\n" + "Scade:" + self.formatta_datastring_italia(e.datapubbto)
        return msg

    def post_messages(self,comune):
        try:
            entrylisttopost = self.get_messages_to_post(comune)
            logging.info("Trovati messaggi da selezionare per " + comune + ": " + str(entrylisttopost.count()))
            if entrylisttopost.count() > 0: 
                messaggio = self.create_fb_post(entrylisttopost)
                if messaggio == '': return 0
                fbmsgid = self.post_to_fb(messaggio)
                link = self.crea_link_to_fbpost(fbmsgid)
                try:
                    msgtw = self.crea_msg_tw(messaggio,link)
                    self.post_to_tw(msgtw)
                except Exception:
                    traceback.print_exc()
                self.update_db_posted(entrylisttopost,fbmsgid)
                return 1
            else :
                return 0
        except Exception :
            traceback.print_exc()
            logging.error("Errore durante la procedura post_messages")
            return 0
            
    def post_to_fb(self,message):
        #AUTENTICAZIONE IN 2 FASI, DA RIPETERE SE SI CAMBIA LA PROPRIA PASSW DI FB COPIAINCOLLANDO i 2 URL (nel secondo url occorre modificare il code restituito dal primo). Il seconso URL restituisce l'access token da passare a graph api
        #prendo il code con interazione utente
        #https://www.facebook.com/dialog/oauth?client_id=<il tuo client id>&redirect_uri=http://albopretoriobassa.appspot.com/&scope=manage_pages,publish_actions,photo_upload,publish_stream,status_update,share_item,offline_access&state=coccomaro
        #scambio il code con un user access token server side
        #https://graph.facebook.com/oauth/access_token?client_id=<il tuo client id>&redirect_uri=http://albopretoriobassa.appspot.com/&client_secret=2e0d11df424ea5f2ffcb745fc06b5add&code=<il code restituito dall'url precedente>
        #dopo avere ottenuto un access token di 2 mesi seguendo l'autenticazione in 2 fasi sopra, ho utilizzato l'access token stesso per eseguire la seguente query e avere un access token perpetuo :
        #user_id = <il tuo user id di fb>
        #access_token 2 mesi :<il risultato del URL precedente>
        #https://graph.facebook.com/<il tuo user id di fb>/accounts?access_token=<il risultato del URL precedente>

        #access token perpetuo,risultato dell'URL sopra : <access token perpetuo>       

        id_msg = "abc"
        logging.info("Ready to post :")
        logging.info(message)
        
        messaggio = message.encode('ascii','ignore')
        graph = facebook.GraphAPI(" <access token perpetuo> ")
        attachment = {}
        
        id_msg = graph.put_wall_post(messaggio,attachment, "<id della pagina fb>")
        result = json.dumps(id_msg)
        ident = json.loads(result)
        logging.info(ident["id"])
                
           
        logging.info("Postato il messaggio con ID " + ident["id"])
        logging.info("Spedito e memorizzato in db!")
        return ident["id"] 
            
    def post_to_tw(self,message):
        #ATTENZIONE POSTA SUL PROFILO DI ECCOILMORO!!!

        #users = api.GetFriends()
        #print [u.name for u in users]

        #POSTA SU BASSA ROMAGNA
        logging.info("Posto su twitter: " + message)
        api = twitter.Api(consumer_key='<consumer key>',consumer_secret='<consumer secret>', access_token_key='<access toke key>', access_token_secret='<access token secret>', cache = None) 
        status = api.PostUpdate(message)
        return
 
    def crea_msg_tw(self, message, link_to_fb):
        msg = message.replace("**","#")
        resto = 100 - len(msg)
        if resto < 0:
            msg = msg[:99]+ "\n"+link_to_fb
        else :
            msg = msg+ "\n"+link_to_fb
        return msg

    
    def crea_link_to_fbpost(self, fbmsg_id):
        link = "http://www.facebook.com/"+fbmsg_id
        slink = self.create_short_url(link)
        return slink


    def get(self):
        posts_per_run = 0
        
        posts_per_run = posts_per_run + self.post_messages("LUGO")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("BAGNACAVALLO")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("ALFONSINE")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("FUSIGNANO")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("MASSA LOMBARDA")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("S.AGATA")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("BAGNARA")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("CONSELICE")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("COTIGNOLA")
        if posts_per_run == self.MAX_POSTS_PER_RUN :
            return
        posts_per_run = posts_per_run + self.post_messages("UNIONE")
      
class FbPostRemover(webapp2.RequestHandler):

    #si aspetta un oggetto di tipo date e retituisce una stringa gg/mm/aaaa
    def formatta_data_italia(self,data):
        mese = data.month
        if mese < 10:
            mese = "0" + str(mese)
        giorno = data.day
        if giorno < 10:
            giorno = "0"+ str(giorno)
        return str(giorno) + "/" + str(mese) + "/" + str(data.year)

    #trasforma una data gg/mm/aaaa in una data aaaammgg
    def formatta_data_sortable(self, data): 
        return data[6:10]+data[3:5]+data[0:2]
    
    def get(self):
        oggi = self.formatta_data_sortable(self.formatta_data_italia(date.today()))
        posted = db.GqlQuery("SELECT * FROM PostedEntry WHERE scadenza< :1",oggi)
        posted.run()
        graph = facebook.GraphAPI("<il token perpetuo>")
        for e in posted:
            try:
                logging.info("Sto per cancellare il  messaggio " + e.fb_messageid)
                graph.delete_object(e.fb_messageid)
                logging.info("Cancellato messaggio " + e.fb_messageid)
            except Exception:
                traceback.print_exc()
                logging.error("Errore in cancellazione!!!, continuo..")
                continue
        logging.info("Finito di cancellare")
        

    
  
class ScraperRunner(webapp2.RequestHandler):
    
    def get(self):
        logging.getLogger().setLevel(logging.DEBUG)
        br = mechanize.Browser()
        br.open("https://scraperwiki.com/login/")
        assert br.viewing_html()
        print br.title()
        #for f in br.forms():
        #    print ("Ecco il form \n")
        #    print f.id
        br.select_form(nr=2)
        br["user_or_email"] = "<il mio username>"
        br["password"] = "<la mia password>"
        res = br.submit()
        logging.info("Logged in")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_cotignola_1/run/")
        logging.info("Scraped Cotignola")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_alfonsine_1/run/")
        logging.info("Scraped Alfonsine")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_conselice_1/run/")
        logging.info("Scraped conselice")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_bagnara_1/run/")
        logging.info("Scraped Bagnara")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_santagata_1/run/")
        logging.info("Scraped S.Agata")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_massa_1/run/")
        logging.info("Scraped Massa")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_fusignano/run/")
        logging.info("Scraped Fusignano")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_bagnacavallo/run/")
        logging.info("Scraped Bagnacavallo")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_lugo_2/run/")
        logging.info("Scraped Lugo")
        br.open("https://scraperwiki.com/scrapers/schedule-scraper/albo_unione/run/")
        logging.info("Scraped Unione")

    

        
app = webapp2.WSGIApplication([('/', MainHandler),('/scrape', ScraperRunner),('/spazzino', FbPostRemover),('/getdata', DataGetter),('/postdata', MessagePoster)],debug=True)
