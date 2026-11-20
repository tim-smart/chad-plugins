import { Cache, Config, Effect, Exit, Redacted, Schema, Stream } from "effect"
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/http"

type Api = "gmail" | "calendar"

export class GoogleReadError extends Schema.TaggedError<GoogleReadError>()(
  "GoogleReadError",
  {
    message: Schema.String,
    uncertain: Schema.optionalKey(Schema.Boolean),
  },
) {}

const error = (message: string) => new GoogleReadError({ message })
const networkError = () =>
  error("Google request failed, timed out or returned invalid data.")
const setting = (key: string) =>
  Config.Redacted(key).pipe(Config.withDefault(Redacted.make("")))

const Token = Schema.Struct({
  access_token: Schema.RedactedFromValue(
    Schema.String.check(Schema.isPattern(/^[!-~]+$/), Schema.isMaxLength(8192)),
  ),
  expires_in: Schema.optional(Schema.Finite),
  scope: Schema.optional(Schema.String),
})

const parseJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(Schema.Unknown),
)

const readJson = Effect.fnUntraced(function* (
  response: HttpClientResponse.HttpClientResponse,
  cap: number,
) {
  const tooLarge = () => error("Google response exceeded the safe size limit.")
  if (Number(response.headers["content-length"]) > cap) {
    return yield* tooLarge()
  }
  const decoder = new TextDecoder()
  let bytes = 0
  let text = ""
  yield* response.stream.pipe(
    Stream.mapError(networkError),
    Stream.runForEach((chunk) => {
      bytes += chunk.byteLength
      if (bytes > cap) return Effect.fail(tooLarge())
      text += decoder.decode(chunk, { stream: true })
      return Effect.void
    }),
  )
  return yield* parseJson(text + decoder.decode()).pipe(
    Effect.mapError(networkError),
  )
})

function validPath(api: Api, path: string) {
  if (api === "gmail") {
    return (
      path === "settings/sendAs" ||
      /^(?:labels|messages|(?:messages|threads)\/[A-Za-z0-9_-]{1,128})$/.test(
        path,
      )
    )
  }
  if (path === "users/me/calendarList") return true
  const [prefix, id, suffix, ...rest] = path.split("/")
  if (
    prefix !== "calendars" ||
    !id ||
    id.length > 9216 ||
    suffix !== "events" ||
    rest.length
  ) {
    return false
  }
  try {
    const raw = decodeURIComponent(id)
    return (
      raw.length <= 1024 &&
      raw !== "." &&
      raw !== ".." &&
      encodeURIComponent(raw) === id
    )
  } catch {
    return false
  }
}

const gmailBase = "https://gmail.googleapis.com/gmail/v1/users/me/"
const calendarBase = "https://www.googleapis.com/calendar/v3/"

const makeTransport = Effect.fnUntraced(function* (
  permission:
    | "gmail.compose"
    | "gmail.readonly"
    | "calendar.readonly"
    | "calendar.events",
  refreshKey = permission === "calendar.events"
    ? "writeRefreshToken"
    : permission === "gmail.compose"
      ? "composeRefreshToken"
      : "refreshToken",
) {
  const write =
    permission === "calendar.events" || permission === "gmail.compose"
  const scope = "https://www.googleapis.com/auth/" + permission
  const setup =
    "Owner: authorize offline access with " +
    permission +
    " and configure clientId, clientSecret and " +
    refreshKey +
    " via the secret store."
  const clientId = yield* setting("clientId")
  const clientSecret = yield* setting("clientSecret")
  const refreshToken = yield* setting(refreshKey)
  const client = HttpClient.withScope(yield* HttpClient.HttpClient)

  // Chad's Undici dispatcher returns 3xx directly; disable Fetch's own redirects too.
  const execute = (req: HttpClientRequest.HttpClientRequest) =>
    client.execute(req).pipe(
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
      Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
      Effect.mapError(networkError),
    )

  const refresh = Effect.fnUntraced(
    function* () {
      const form = {
        client_id: Redacted.value(clientId),
        client_secret: Redacted.value(clientSecret),
        refresh_token: Redacted.value(refreshToken),
        grant_type: "refresh_token",
        scope,
      }
      if (!form.client_id || !form.client_secret || !form.refresh_token) {
        return yield* error(setup)
      }
      const response = yield* execute(
        HttpClientRequest.post("https://oauth2.googleapis.com/token").pipe(
          HttpClientRequest.bodyUrlParams(form),
        ),
      )
      if (response.status < 200 || response.status >= 300) {
        return yield* error(
          "OAuth refresh failed (HTTP " + response.status + "). " + setup,
        )
      }
      const token = yield* Schema.decodeUnknownEffect(Token)(
        yield* readJson(response, 32768),
      ).pipe(
        Effect.mapError(() =>
          error("Google returned no usable access token. " + setup),
        ),
      )
      if ((write || token.scope !== undefined) && token.scope !== scope) {
        return yield* error("Unexpected authorization scope. " + setup)
      }
      return {
        value: token.access_token,
        ttl: Math.max(0, Math.min(token.expires_in ?? 0, 3600) - 60) * 1000,
      }
    },
    Effect.scoped,
    Effect.timeoutOrElse({
      duration: "10 seconds",
      orElse: () => Effect.fail(networkError()),
    }),
  )

  const tokens = yield* Cache.makeWith((_key: string) => refresh(), {
    capacity: 1,
    timeToLive: (exit) => (Exit.isSuccess(exit) ? exit.value.ttl : 0),
  })
  const accessToken = Cache.get(tokens, permission)
  const invalidate = (token: Effect.Success<typeof accessToken>) =>
    Cache.invalidateWhen(tokens, permission, (current) => current === token)

  return { execute, accessToken, invalidate }
})

export const makeGoogleRead = Effect.fnUntraced(function* (api: Api) {
  const gmail = api === "gmail"
  const name = gmail ? "Gmail" : "Calendar"
  const base = gmail ? gmailBase : calendarBase
  const { execute, accessToken, invalidate } = yield* makeTransport(
    gmail ? "gmail.readonly" : "calendar.readonly",
  )

  const read = Effect.fnUntraced(function* (
    req: HttpClientRequest.HttpClientRequest,
  ) {
    const response = yield* execute(req)
    return {
      status: response.status,
      body:
        response.status >= 200 && response.status < 300
          ? yield* readJson(response, gmail ? 8 * 1024 * 1024 : 512 * 1024)
          : undefined,
    }
  }, Effect.scoped)

  const get = Effect.fnUntraced(
    function* (path: string, params: URLSearchParams) {
      if (!validPath(api, path)) {
        return yield* error("Invalid Google read path.")
      }
      const req = HttpClientRequest.get(base + path + "?" + params.toString())
      for (let attempt = 0; ; attempt++) {
        const token = yield* accessToken
        const { status, body } = yield* read(
          HttpClientRequest.bearerToken(req, token.value),
        )
        if (status >= 200 && status < 300) return body
        if (status === 401) {
          yield* invalidate(token)
          if (attempt === 0) continue
        }
        return yield* error(
          name +
            " read failed (HTTP " +
            status +
            ")." +
            (status === 401 || status === 403
              ? " Check " +
                name +
                " API access and " +
                api +
                ".readonly authorization."
              : ""),
        )
      }
    },
    Effect.timeoutOrElse({
      duration: "45 seconds",
      orElse: () => Effect.fail(networkError()),
    }),
  )

  return { get }
})

// Only opaque path segments are accepted; never accept URLs, paths or query strings.
const segment = Schema.decodeUnknownEffect(
  Schema.String.check(
    Schema.isBetweenLength(1, 1024),
    Schema.isPattern(/^[A-Za-z0-9@._+~-]+$/),
    Schema.makeFilter((id) => id !== "." && id !== ".."),
  ),
)
const encodeId = (id: string) =>
  segment(id).pipe(
    Effect.map(encodeURIComponent),
    Effect.mapError(() => error("Invalid Calendar write identifier.")),
  )
const ambiguous = () =>
  error(
    "Calendar write outcome is unknown. Check the calendar before retrying.",
  )

export const makeCalendarWrite = Effect.fnUntraced(function* () {
  const { execute, accessToken, invalidate } =
    yield* makeTransport("calendar.events")

  const mutate = Effect.fnUntraced(function* (
    method: "POST" | "PATCH" | "DELETE",
    calendarId: string,
    eventId: string | undefined,
    body?: Record<string, unknown>,
  ): Effect.fn.Return<unknown, GoogleReadError> {
    if (
      (method !== "POST" && method !== "PATCH" && method !== "DELETE") ||
      (method === "POST") !== (eventId === undefined) ||
      (method === "DELETE" && body !== undefined)
    ) {
      return yield* error("Invalid Calendar write request.")
    }
    const path =
      "calendars/" +
      (yield* encodeId(calendarId)) +
      "/events" +
      (eventId === undefined ? "" : "/" + (yield* encodeId(eventId)))
    let req = HttpClientRequest.make(method)(calendarBase + path)
    if (body !== undefined) {
      req = yield* HttpClientRequest.bodyJson(req, body).pipe(
        Effect.mapError(() => error("Invalid Calendar event body.")),
      )
    }
    const token = yield* accessToken
    return yield* Effect.gen(function* () {
      const response = yield* execute(
        HttpClientRequest.bearerToken(req, token.value),
      ).pipe(Effect.mapError(ambiguous))
      if (response.status === 204) return undefined
      if (response.status >= 200 && response.status < 300) {
        return yield* readJson(response, 512 * 1024).pipe(
          Effect.mapError(ambiguous),
        )
      }
      if (response.status === 401) yield* invalidate(token)
      if (response.status === 408 || response.status >= 500) {
        return yield* ambiguous()
      }
      return yield* error(
        "Calendar write failed (HTTP " + response.status + ").",
      )
    }).pipe(
      Effect.scoped,
      Effect.timeoutOrElse({
        duration: "45 seconds",
        orElse: () => Effect.fail(ambiguous()),
      }),
    )
  })

  return { mutate }
})

const GmailIdResponse = Schema.Struct({
  id: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{1,128}$/)),
})

const SendAsResponse = Schema.Struct({
  sendAs: Schema.Array(
    Schema.Struct({
      sendAsEmail: Schema.String,
      isPrimary: Schema.optionalKey(Schema.Boolean),
      verificationStatus: Schema.optionalKey(Schema.String),
    }),
  ),
})

const verifySendAs = Effect.fnUntraced(function* (
  reader: Effect.Success<ReturnType<typeof makeGoogleRead>>,
  fromAddress: string,
) {
  const data = yield* reader.get(
    "settings/sendAs",
    new URLSearchParams({
      fields: "sendAs(sendAsEmail,isPrimary,verificationStatus)",
    }),
  )
  const { sendAs } = yield* Schema.decodeUnknownEffect(SendAsResponse)(
    data,
  ).pipe(Effect.mapError(networkError))
  const primary = sendAs.find((alias) => alias.isPrimary)
  const verified = sendAs.some(
    (alias) =>
      alias.sendAsEmail.toLowerCase() === fromAddress.toLowerCase() &&
      (alias.isPrimary || alias.verificationStatus === "accepted"),
  )
  if (
    !primary ||
    !/^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+$/.test(primary.sendAsEmail) ||
    !verified
  ) {
    return yield* error(
      `Gmail write blocked: verify ${fromAddress} in Send mail as settings.`,
    )
  }
  return primary.sendAsEmail
})

export const makeGmailCompose = Effect.fnUntraced(function* (options?: {
  readonly reader?: Effect.Success<ReturnType<typeof makeGoogleRead>>
  readonly from?: string
}) {
  const { execute, accessToken, invalidate } =
    yield* makeTransport("gmail.compose")
  const unknownOutcome = () =>
    new GoogleReadError({
      uncertain: true,
      message:
        "Gmail write outcome unknown. Check Drafts and Sent before trying again.",
    })

  const write = Effect.fnUntraced(function* (
    kind: "draft" | "send",
    raw: string,
  ) {
    const token = yield* accessToken
    const user =
      options?.from && options.reader
        ? encodeURIComponent(yield* verifySendAs(options.reader, options.from))
        : "me"
    const endpoint = kind === "draft" ? "drafts" : "messages/send"
    const payload = kind === "draft" ? { message: { raw } } : { raw }
    const req = yield* HttpClientRequest.post(
      `https://gmail.googleapis.com/gmail/v1/users/${user}/${endpoint}?fields=id`,
    ).pipe(
      HttpClientRequest.bodyJson(payload),
      Effect.mapError(() => error("Invalid Gmail write body.")),
    )

    return yield* Effect.gen(function* () {
      const response = yield* execute(
        HttpClientRequest.bearerToken(req, token.value),
      ).pipe(Effect.mapError(unknownOutcome))
      if (response.status === 401) yield* invalidate(token)
      if (response.status === 408 || response.status >= 500) {
        return yield* unknownOutcome()
      }
      if (response.status < 200 || response.status >= 300) {
        return yield* error(
          `Gmail write failed (HTTP ${response.status}). Check the mailbox and compose authorization.`,
        )
      }
      return yield* readJson(response, 4096).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(GmailIdResponse)),
        Effect.mapError(unknownOutcome),
      )
    }).pipe(
      Effect.scoped,
      Effect.timeoutOrElse({
        duration: "15 seconds",
        orElse: () => Effect.fail(unknownOutcome()),
      }),
    )
  })

  return {
    createDraft: (raw: string) => write("draft", raw),
    sendMessage: (raw: string) => write("send", raw),
  }
})
