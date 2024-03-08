open Core
module StringSet = Stdlib.Set.Make (String)
module T = Domainslib.Task

let get_weather_stats_table_fork filename start stop =
  let parse_line_exn line =
    let pieces = String.split ~on:';' line in
    match pieces with
    | [ l; t ] -> l, Hashtbl.find_exn Float_utils.float_table t
    | _ -> raise (Invalid_argument "")
  in
  let table = Hashtbl.create (module String) ~growth_allowed:true ~size:50_000 in
  let channel = In_channel.create filename in
  let pos = ref start in
  let line = ref "" in
  let line_length = ref 0L in
  (* Move to starting place in file before reading. *)
  In_channel.seek channel start;
  while Int64.( < ) !pos stop do
    try
      line := In_channel.input_line_exn channel;
      line_length := Int64.( + ) (Int64.of_int (String.length !line)) 1L;
      pos := Int64.( + ) !pos !line_length;
      let loc, temp = parse_line_exn !line in
      if Hashtbl.mem table loc
      then (
        (* Get the existing record and update it with the new value. *)
        let existing_result = Core.Hashtbl.find_exn table loc in
        Record.Record.update_record existing_result temp)
      else (
        let new_result = Record.Record.create_record temp in
        Core.Hashtbl.add_exn table ~key:loc ~data:new_result)
    with
    | End_of_file -> pos := stop
  done;
  In_channel.close channel;
  table
;;

let get_weather_stats_table filename pool domains =
  let merge_into_custom ~src ~dst =
    Hashtbl.iteri src ~f:(fun ~key ~data ->
      match Hashtbl.find dst key with
      | Some existing -> Record.Record.merge_record existing data
      | None -> Hashtbl.add_exn dst ~key ~data);
    ()
  in
  match domains, pool with
  | None, None ->
    let start = 0L in
    let stop = In_channel.length (In_channel.create filename) in
    get_weather_stats_table_fork filename start stop
  | Some domains', Some pool' ->
    let master_table =
      Hashtbl.create (module String) ~growth_allowed:false ~size:500_000
    in
    let chunks = File_utils.Filechunks.get_file_chunks filename domains' in
    let num_chunks = Array.length chunks in
    Domainslib.Task.parallel_for pool' ~start:0 ~finish:(num_chunks - 1) ~body:(fun i ->
      let start, stop = chunks.(i) in
      let table = get_weather_stats_table_fork filename start stop in
      merge_into_custom ~src:table ~dst:master_table);
    master_table
  | _ -> raise (Invalid_argument "Invalid arguments")
;;

let get_weather_stats ~filename ~use_std_out ~num_domains =
  let pool = T.setup_pool ~num_domains () in
  let table =
    T.run pool (fun _ ->
      get_weather_stats_table filename (Some pool) (Some (Int64.of_int_exn num_domains)))
  in
  (* Output *)
  let keys = Hashtbl.keys table in
  let total_names = List.length keys in
  let sorted_names = StringSet.of_list keys in
  let output_filename = File_utils.Filename.get_output_filename filename in
  let output = if use_std_out then stdout else Out_channel.create output_filename in
  let iter_count = ref 0 in
  Out_channel.output_char output '{';
  StringSet.iter
    (fun name ->
      let rounding = Some 1 in
      let r = Core.Hashtbl.find_exn table name in
      let min = Float_utils.round_dfrac rounding r.min in
      let average = Float_utils.round_dfrac rounding (r.total /. r.count) in
      let max = Float_utils.round_dfrac rounding r.max in
      iter_count := !iter_count + 1;
      if !iter_count = total_names
      then
        Out_channel.output_string
          output
          (Printf.sprintf "%s=%0.1f/%0.1f/%0.1f" name min average max)
      else
        Out_channel.output_string
          output
          (Printf.sprintf "%s=%0.1f/%0.1f/%0.1f, " name min average max))
    sorted_names;
  Out_channel.output_char output '}';
  Out_channel.close output
;;
