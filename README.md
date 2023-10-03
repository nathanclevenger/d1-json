# d1-json
JSON CRUD + Search Functions for Cloudflare D1

```bash
npm i d1-json
```

```javascript
import { DB } from 'd1-json'

export default {
  fetch: (req, env, ctx) => {
    const db = DB(env)
    const timestamp = Date.now()
    const { hostname, pathname, search } = new URL(url)
    const { colo, city, country, postalCode } = req.cf
    const headers = JSON.fromEntries(req.headers)
    const ip = headers['cf-ip']
    const event = { timestamp, url, method, ip, headers, colo, city, country, postalCode }
    ctx.waitUntil(db.logs.insert(event))
    return Response.json(event)
  }
}
```



