= let
    dur = #duration(0,0,0,[SecondsColumn]),
    h = Duration.Hours(dur),
    m = Duration.Minutes(dur),
    s = Duration.Seconds(dur),
    parts = {
        if h <> 0 then Text.From(h) & "h" else null,
        if m <> 0 then Text.From(m) & "m" else null,
        if s <> 0 then Text.From(s) & "s" else null
    }
  in
    Text.Combine(List.RemoveNulls(parts), " ")