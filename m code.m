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


=TRIM(SUBSTITUTE(SUBSTITUTE(TEXT(A1/86400,"[h]\h m\m s\s"),"0h ",""),"0m ",""))

=TRIM(IF(QUOTIENT(B8,3600)>0, QUOTIENT(B8,3600)&"h ","") &
      IF(QUOTIENT(MOD(B8,3600),60)>0, QUOTIENT(MOD(B8,3600),60)&"m ","") &
      MOD(B8,60)&"s")