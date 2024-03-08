let round x = floor (x +. 0.5)

let round_dfrac d x =
  match d with
  | None -> x
  | Some d ->
    if x -. round x = 0.
    then x
    else (
      (* x is an integer. *)
      let m = 10. ** float d in
      (* m moves 10^-d to 1. *)
      floor ((x *. m) +. 0.5) /. m)
;;

let float_table =
  let open Core in
  let table = Hashtbl.create (module String) ~growth_allowed:true ~size:10_000 in
  let float = ref 0. in
  while Float.( < ) !float 100. do
    let float_val = !float in
    let str = Printf.sprintf "%0.1f" float_val in
    Hashtbl.add_exn table ~key:str ~data:float_val;
    Hashtbl.add_exn table ~key:("-" ^ str) ~data:(Float.( * ) float_val (-1.));
    float := float_val +. 0.1
  done;
  table
;;
