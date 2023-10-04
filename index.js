import { base62 } from './utils.js'
import camelcaseKeys from 'camelcase-keys'

export const DB = env => {
  if (!env) throw new Error('No env provided')
  let db
  // is it a database?
  if (!!env[key].prepare) {
    db = env
  } else {
    // find the first database
    db = Object.values(env).find(db => !!db.prepare)
  }

  db.init = async () => {
  const statement = `create table if not exists data (
    type text,
    id text not null,
    data text,
    createdAt datetime default current_timestamp,
    createdBy text,
    createdIn text,
    updatedAt datetime default current_timestamp,
    updatedBy text,
    updatedIn text,
    primary key (type,id)
  );`
  const initResults = await db.prepare(statement).run()
  console.log({ initResults })
  }

  db.types = async () => {
  const statement = `select distinct type from data`
  const results = await db.prepare(statement).all()
  return results.map(({ type }) => type)
  }

  db.count = async () => {
  const statement = `select count(*) from data`
  const results = await db.prepare(statement).get()
  return results['count(*)']
  }

  db.get = async (id) => {
  const statement = `select * from data where id = ?`
  const { success, meta, results } = await db.prepare(statement).bind(id).all()
  results.map(item => item.data = JSON.parse(item.data))
  console.log({ db: key, id, meta, results })
  return { success, data: results, meta: camelcaseKeys(meta) }
  }

  return new Proxy(db, {
    get: (target, prop) => {
      if (prop in target) return target[prop]
      const type = prop
      return {
        list: async ({ order = 'id', offset = 0, limit = 100 } = {}) => {
          const statement = `select * from data where type = ? order by ? limit ? offset ?`
          const { success, meta, results } = await db.prepare(statement).bind(type, order, limit, offset).all()
          const data = results.map(item => ({ _id: item.id, ...JSON.parse(item.data)}))
          return { success, data, meta: camelcaseKeys(meta) }
        },
        find: async (query = {}, { order = 'id', offset = 0, limit = 100 } = {}) => {
          // const statement = `select * from data where type = ? ${Object.entries(query).map(([key, value]) => `and data->'$.${key}' = ${value}`)} order by ${sort} limit ${limit} offset ${offset}`
          const statement = `select * from data where type = ?${Object.keys(query)?.map(key => ` and data->'$.${key}' = ?`).join('')} order by ? limit ? offset ?`
          const args = [type, ...Object.values(query)?.map(i => JSON.stringify(i)), order, limit, offset]
          console.log({ statement, type, query, order, limit, offset, keys: Object.keys(query), values: Object.values(query), args })
          const { success, meta, results } = await db.prepare(statement).bind(...args).all()
          const data = results?.map(item => ({ _id: item.id, ...JSON.parse(item.data)}))
          return { success, data, meta: camelcaseKeys(meta) }
        },
        findOne: async (query) => {
          const statement = `select * from data where type = ? ${Object.keys(query).map(key => ` and data->>'$.${key}' = ?`).join('')} limit 1`
          const results = await db.prepare(statement).bind(type, ...Object.values(query)).all()
          return results
        },
        get: async (id) => {
          const statement = `select * from data where type = ? and id = ?`
          const { success, results, meta } = await db.prepare(statement).bind(type, id).all()
          results.map(item => item.data = JSON.parse(item.data))
          console.log({ db: key, type, id, meta, results })
          return { success, ...results[0], meta: camelcaseKeys(meta) }
        },
        getOrCreate: async (id, data) => {
          const statement = `select * from data where type = ? and id = ?`
          const response = await db.prepare(statement).bind(type, id).all()
          if (response.results.length == 0) {
            const createStatement = `insert into data (type, id, data) values (?, ?, ?) on conflict (type, id) do update set data = json_patch(data, ?)`
            const { success, meta, ...createResults } = await db.prepare(createStatement).bind(type, id, JSON.stringify(data), JSON.stringify(data)).run()
            return { success, type, id, data, ...createResults, meta: camelcaseKeys(meta) }
          }
          const { success, results, meta } = response
          results.map(item => item.data = JSON.parse(item.data))
          console.log({ db: key, type, id, meta, results })
          return { success, ...results[0], meta: camelcaseKeys(meta) }
        },
        set: async (id, data) => {
          const statement = `insert into data (type, id, data) values (?, ?, ?) on conflict (type, id) do update set data = json_patch(data, ?)`
          const { success, results, meta } = await db.prepare(statement).bind(type, id, JSON.stringify(data), JSON.stringify(data)).run()
          return { success, results, meta: camelcaseKeys(meta) }
        },
        overwrite: async (id, data) => {
          const statement = `update data set data = ? where type = ? and id = ?`
          const results = await db.prepare(statement).bind(JSON.stringify(data), type, id).run()
          return camelcaseKeys(results, { deep: true })
        },
        delete: async (id) => {
          const statement = `delete from data where type = ? and id = ?`
          const results = await db.prepare(statement).bind(type, id).run()
          return camelcaseKeys(results, { deep: true })
        },
        insert: async (id, data) => {
          const statement = `insert into data (type, id, data) values (?, ?, ?)`
          const results = await db.prepare(statement).bind(type, id, JSON.stringify(data)).run()
          return results
        },
        insertMany: async (data, { id } = {}) => {
          const statement = `insert into data (type, id, data) values ${data.map(({ id }) => `(?, ?, ?)`).join(',')}`
          const results = await db.prepare(statement).bind(...data.flatMap(data => [type, id ? data[id] : (data._id ?? data.id ?? base62(8)), JSON.stringify(data)])).run()
          return results
        },
        upsert: async (id, data) => {
          const statement = `insert into data (type, id, data) values (?, ?, ?) on conflict (type, id) do update set data = json_patch(data, ?)`
          const results = await db.prepare(statement).bind(type, id, JSON.stringify(data), JSON.stringify(data)).run()
          return results
        },
        upsertMany: async (data, { id } = {}) => {
          const statement = `insert into data (type, id, data) values ${data.map(({ id }) => `(?, ?, ?)`).join(',')} on conflict (type, id) do update set data = excluded.data`
          const results = await db.prepare(statement).bind(...data.flatMap(data => [type, id ? data[id] : (data._id ?? data.id ?? base62(8)), JSON.stringify(data)])).run()
          return results
        },
        update: async (id, data) => {
          const statement = `update data set data = json_patch(data, ?) where type = ? and id = ?`
          const results = await db.prepare(statement).bind(JSON.stringify(data), type, id).run()
          return results
        },
        count: async (query) => {
          const statement = `select count(*) from data where type = ? ${Object.keys(query).map(key => ` and data->>'$.${key}' = ?`).join('')}`
          const results = await db.prepare(statement).bind(type, ...Object.values(query)).all()
          return camelcaseKeys(results, { deep: true })
        },
      }
    }
  })
}

export const withDB = (req, env, ctx) => {
  Object.keys(env).map(key => {
    if (!!env[key].prepare) {
      env[key] = DB(env[key])
    }
  })
}
