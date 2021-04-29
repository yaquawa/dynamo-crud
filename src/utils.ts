export function omit<T extends Record<string, any>, Key extends keyof T>(obj: T, keys: Key[]): Omit<T, Key> {
  const cloneObj = { ...obj }

  for (const key of keys) {
    if (key in obj) {
      delete cloneObj[key]
    }
  }

  return cloneObj
}

/**
 * `Promise.all` will gather the resolved values which may cost unnecessary memory.
 * Use this one if you're not going to use the gathered resolved values.
 * @param promises
 */
export function runParallel(promises: Promise<any>[]): Promise<void> {
  return new Promise((resolve, reject) => {
    let remainingCount = promises.length

    for (const promise of promises) {
      promise
        .then(() => {
          if (--remainingCount === 0) {
            resolve()
          }
        })
        .catch((error) => {
          reject(error)
        })
    }
  })
}
