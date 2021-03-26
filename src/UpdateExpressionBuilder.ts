import { marshall } from '@aws-sdk/util-dynamodb'
import { GetTypeByPath } from './types'
import { SetType } from './types'
import { ExtractPathExpressions } from './types'

type PlusMinusOperator = '+' | '-'
type Actions<T = any> = { path: string; value: T }[]

export class UpdateExpressionBuilder<Model extends Record<string, any>> {
  private addActions: Actions = []
  private setActions: Actions = []
  private deleteActions: Actions<SetType> = []
  private removeActions: string[] = []
  private setCalcActions: { path: string; path1: string; path2: string; operator: PlusMinusOperator }[] = []
  private setIfNotExistsActions: Actions = []
  private setListAppendActions: Actions<any[]> = []

  add<Path extends ExtractPathExpressions<Model, number | Set<any>>>(
    path: Path,
    value: GetTypeByPath<Model, Path>
  ): this {
    this.addActions.push({ path: path as string, value })

    return this
  }

  delete<Path extends ExtractPathExpressions<Model, Set<any>>>(
    path: Path,
    value: GetTypeByPath<Model, Path>
  ): this {
    this.deleteActions.push({ path: path as string, value: value as SetType })

    return this
  }

  set<Path extends ExtractPathExpressions<Model>>(path: Path, value: GetTypeByPath<Model, Path>): this {
    this.setActions.push({ path: path as string, value })

    return this
  }

  setCalc<Path extends ExtractPathExpressions<Model, number>>(
    path: Path,
    path1: Path,
    operator: PlusMinusOperator,
    path2: Path
  ): this {
    this.setCalcActions.push({
      path: path as string,
      path1: path1 as string,
      path2: path2 as string,
      operator,
    })

    return this
  }

  setIfNotExists<Path extends ExtractPathExpressions<Model>>(
    path: Path,
    value: GetTypeByPath<Model, Path>
  ): this {
    this.setIfNotExistsActions.push({
      path: path as string,
      value,
    })

    return this
  }

  setListAppend<Path extends ExtractPathExpressions<Model, any[]>>(
    path: Path,
    value: GetTypeByPath<Model, Path>
  ): this {
    this.setListAppendActions.push({
      path: path as string,
      value: value as any[],
    })

    return this
  }

  remove<Path extends ExtractPathExpressions<Model>>(path: Path): this {
    this.removeActions.push(path as string)

    return this
  }

  compile() {
    let attributeValuePlaceholderId = 0
    let attributeNamePlaceholderId = 0

    let updateExpression: string[] = []
    let expressionAttributeValues: Record<string, any> = {}
    let expressionAttributeNames: Record<string, string> = {}

    const getPathPlaceholder = (path: string) => {
      const pathPlaceholder = `#p${attributeNamePlaceholderId++}`
      expressionAttributeNames[pathPlaceholder] = path

      return pathPlaceholder
    }

    const getValuePlaceholder = (value: any) => {
      const valuePlaceholder = `:v${attributeValuePlaceholderId++}`
      expressionAttributeValues[valuePlaceholder] = value

      return valuePlaceholder
    }

    if (
      this.setActions.length ||
      this.setCalcActions.length ||
      this.setIfNotExistsActions.length ||
      this.setListAppendActions.length
    ) {
      const setActions = this.setActions.map(({ path, value }) => {
        const pathPlaceholder = getPathPlaceholder(path)
        const valuePlaceholder = getValuePlaceholder(value)

        return `${pathPlaceholder} = ${valuePlaceholder}`
      })

      const setCalcActions = this.setCalcActions.map(({ path, path1, path2, operator }) => {
        const pathPlaceholder = getPathPlaceholder(path)
        const path1Placeholder = getPathPlaceholder(path1)
        const path2Placeholder = getPathPlaceholder(path2)

        return `${pathPlaceholder} = ${path1Placeholder} ${operator} ${path2Placeholder}`
      })
      const setIfNotExistsActions = this.setIfNotExistsActions.map(({ path, value }) => {
        const pathPlaceholder = getPathPlaceholder(path)
        const valuePlaceholder = getValuePlaceholder(value)

        return `${pathPlaceholder} = if_not_exists(${pathPlaceholder}, ${valuePlaceholder})`
      })

      const setListAppendActions = this.setListAppendActions.map(({ path, value }) => {
        const pathPlaceholder = getPathPlaceholder(path)
        const valuePlaceholder = getValuePlaceholder(value)

        return `${pathPlaceholder} = list_append(${pathPlaceholder}, ${valuePlaceholder})`
      })

      const setExpression =
        'SET ' +
        [...setActions, ...setCalcActions, ...setIfNotExistsActions, ...setListAppendActions].join(', ')

      updateExpression.push(setExpression)
    }

    if (this.removeActions.length) {
      const removeExpression =
        'REMOVE ' +
        this.removeActions
          .map((path) => {
            return getPathPlaceholder(path)
          })
          .join(', ')

      updateExpression.push(removeExpression)
    }

    if (this.addActions.length) {
      const addExpression =
        'ADD ' +
        this.addActions
          .map(({ path, value }) => {
            const pathPlaceholder = getPathPlaceholder(path)
            const valuePlaceholder = getValuePlaceholder(value)

            return `${pathPlaceholder} ${valuePlaceholder}`
          })
          .join(', ')

      updateExpression.push(addExpression)
    }

    if (this.deleteActions.length) {
      const deleteExpression =
        'DELETE ' +
        this.deleteActions
          .map(({ path, value }) => {
            const pathPlaceholder = getPathPlaceholder(path)
            const valuePlaceholder = getValuePlaceholder(value)

            return `${pathPlaceholder} ${valuePlaceholder}`
          })
          .join(', ')

      updateExpression.push(deleteExpression)
    }

    return {
      UpdateExpression: updateExpression.join(`\n`),
      ExpressionAttributeValues: marshall(expressionAttributeValues),
      ExpressionAttributeNames: expressionAttributeNames,
    }
  }
}
