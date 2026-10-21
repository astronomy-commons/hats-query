IVOA XML schemas used to validate the server responses in tests.

| File | Source |
|------|--------|
| `VOSIAvailability-v1.0.xsd` | [ivoa-std/VOSI@e94dad8](https://github.com/ivoa-std/VOSI/blob/e94dad8904b5e1edc36db5b89da291e3b58caf7e/VOSIAvailability-v1.0.xsd) |
| `VOSICapabilities-v1.0.xsd` | [ivoa-std/VOSI@e94dad8](https://github.com/ivoa-std/VOSI/blob/e94dad8904b5e1edc36db5b89da291e3b58caf7e/VOSICapabilities-v1.0.xsd) |
| `VOResource-v1.3.xsd` | [ivoa-std/VOResource@1619cee](https://github.com/ivoa-std/VOResource/blob/1619ceecd32da87a8a06d273d9427a55d82d706d/VOResource-v1.3.xsd) |
| `VODataService-v1.3.xsd` | [ivoa-std/VODataService@bf7aab7](https://github.com/ivoa-std/VODataService/blob/bf7aab7b53f6a586cfa54e2a565a9c6dbd94ce24/VODataService-v1.3.xsd) |
| `TAPRegExt-v1.1.xsd` | [ivoa-std/TAPRegExt@0350097](https://github.com/ivoa-std/TAPRegExt/blob/0350097178adebbaa32fc9873c796df41839cbb3/TAPRegExt-v1.1.xsd) |

The schemas are © IVOA, distributed under
[CC-BY-SA-4.0](https://creativecommons.org/licenses/by-sa/4.0/), unmodified.

`stc-stub.xsd` is not an IVOA schema: it stands in for
[STC 1.30](http://www.ivoa.net/xml/STC/stc-v1.30.xsd), which VODataService imports,
declaring only the `STCResourceProfile` element VODataService references, with any content.
The server doesn't emit STC, so the full schema (and its xlink dependency) is not needed.