module Record : sig
  type t =
    { mutable total : float
    ; mutable count : float
    ; mutable min : float
    ; mutable max : float
    }

  val create_record : float -> t
  val update_record : t -> float -> unit
  val merge_record : t -> t -> unit
end = struct
  type t =
    { mutable total : float
    ; mutable count : float
    ; mutable min : float
    ; mutable max : float
    }

  let create_record num = { total = num; count = 1.; min = num; max = num }

  let update_record result num =
    let min = if Float.compare num result.min < 0 then num else result.min in
    let max = if Float.compare num result.max > 0 then num else result.max in
    let total = result.total +. num in
    let count = result.count +. 1. in
    result.min <- min;
    result.max <- max;
    result.total <- total;
    result.count <- count;
    ()
  ;;

  let merge_record a b =
    let min = if Float.compare a.min b.min < 0 then a.min else b.min in
    let max = if Float.compare a.max b.max > 0 then a.max else b.max in
    let total = a.total +. b.total in
    let count = a.count +. b.count in
    a.min <- min;
    a.max <- max;
    a.total <- total;
    a.count <- count
  ;;
end
