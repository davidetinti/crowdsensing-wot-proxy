# Tipi di dato personalizzati per le campagne

Le campagne possono dichiarare, oltre al campo `type`, un campo opzionale
`data_schema`: un sottoinsieme di JSON Schema (allineato al `DataSchema`
delle W3C WoT Thing Description) che descrive il payload che i device
devono inviare. **Definire un nuovo tipo di dato condivisibile non richiede
modifiche al codice**: basta inserire in MongoDB una campagna con il proprio
schema.

## Come funziona

- **Proxy**: valida ogni submission contro `data_schema`, sia in modalità
  push (`sendPushCampaignData` rifiuta il payload con l'errore di
  validazione) sia in modalità pull (i dati letti dal device che non
  rispettano lo schema vengono scartati e non salvati).
- **App iOS**: se la campagna ha un `data_schema`, l'invio manuale mostra
  un form generato dinamicamente dallo schema (campi numerici con unità e
  limiti, testo, booleani, enum). Per le modalità automatiche l'app usa il
  registro dei sensori (`SensorRegistry`): se il `type` della campagna
  corrisponde a un sensore del dispositivo, i dati vengono prodotti da
  quello.
- **App Android**: stesso comportamento dell'app iOS — form dinamico
  generato dal `data_schema` per l'invio manuale e `SensorRegistry`
  (posizione, batteria, barometro, accelerometro) per le modalità
  automatiche; le modalità proposte sono filtrate in base ai sensori
  presenti sul dispositivo.

## Schema supportato

```jsonc
{
  "type": "object",
  "properties": {
    "<nomeCampo>": {
      "type": "number | integer | string | boolean",
      "title": "Etichetta mostrata nel form",   // opzionale
      "description": "Testo di aiuto",           // opzionale
      "unit": "dB, °C, hPa, ...",                // opzionale
      "minimum": 0,                               // opzionale (numerici)
      "maximum": 130,                             // opzionale (numerici)
      "enum": ["a", "b", "c"]                    // opzionale
    }
  },
  "required": ["<nomeCampo>", "..."]
}
```

## Esempio: campagna sul rumore percepito

```js
db.campaigns.insertOne({
  title: "Rumore nel quartiere",
  description: "Segnala il livello di rumore percepito nella tua zona",
  organization: "Comune di Bologna",
  ideal_submission_interval: 3600,
  points: 50,
  pull_enabled: false,
  push_auto_enabled: false,
  push_input_enabled: true,
  submission_required: 10,
  type: "noise",
  data_schema: {
    type: "object",
    properties: {
      level: {
        type: "integer",
        title: "Livello di rumore",
        unit: "dB stimati",
        minimum: 0,
        maximum: 130,
      },
      source: {
        type: "string",
        title: "Fonte principale",
        enum: ["traffico", "cantiere", "locali", "altro"],
      },
      indoor: { type: "boolean", title: "Ti trovi al chiuso?" },
    },
    required: ["level"],
  },
});
```

## Sensori disponibili sui device

Per le modalità automatiche (pull e push automatico) il `type` della
campagna viene risolto dal registro dei sensori delle app (iOS e Android),
con payload identici tra le due piattaforme:

| `type` | Sensore | Payload prodotto |
|---|---|---|
| `location`, `gps` | CoreLocation / FusedLocationProvider | `{latitude, longitude, accuracy, ...}` |
| `battery` | UIDevice / BatteryManager | `{level, state}` |
| `pressure` | Barometro | `{pressure}` (kPa) |
| `motion` | Accelerometro | `{x, y, z}` (g) |

I tipi senza sensore corrispondente restano utilizzabili in modalità
manuale tramite `data_schema`. Per aggiungere un nuovo sensore lato app è
sufficiente implementare il protocollo/interfaccia `DeviceSensor` e
registrarlo nel `SensorRegistry`:
`ios/CrowdsensingWoT/Services/SensorRegistry.swift` (iOS) e
`app/src/main/java/com/example/crowdsensingwotandroidapp/utils/sensors/`
(Android) nel repository dell'app.
