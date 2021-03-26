export class CommandResult<Data, RawResponse> {
  public readonly data: Data
  public readonly rawResponse: RawResponse
  constructor(data: Data, rawResponse: RawResponse) {
    this.data = data
    this.rawResponse = rawResponse
  }
}
