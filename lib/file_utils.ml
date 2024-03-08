open Core

module Filename : sig
  val get_output_filename : string -> string
end = struct
  let rec get_output_filename_inner pieces =
    match pieces with
    | [] -> ""
    | [ _ ] -> "out"
    | head :: tail -> head ^ "." ^ get_output_filename_inner tail
  ;;

  let get_output_filename filename =
    let open Core in
    get_output_filename_inner (String.split ~on:'.' filename)
  ;;
end

module Filechunks : sig
  val get_file_chunks : string -> int64 -> (int64 * int64) array
end = struct
  let rec get_file_chunks_inner channel start stop chunk_size max acc =
    if Int64.( >= ) start max
    then acc
    else if Int64.( >= ) stop max
    then Array.append acc [| start, max |]
    else (
      (* Stop should be EOL or EOF *)
      let cur_stop = ref stop in
      let not_eol_or_eof = ref true in
      In_channel.seek channel !cur_stop;
      while !not_eol_or_eof do
        let next_byte = In_channel.input_byte channel in
        match next_byte with
        | None ->
          not_eol_or_eof := false;
          cur_stop := In_channel.pos channel
        | Some x ->
          if Int.equal x 10
          then (
            not_eol_or_eof := false;
            cur_stop := In_channel.pos channel)
          else ()
      done;
      let next_start = !cur_stop in
      let next_stop = Int64.( + ) next_start chunk_size in
      get_file_chunks_inner
        channel
        next_start
        next_stop
        chunk_size
        max
        (Array.append acc [| start, !cur_stop |]))
  ;;

  let get_file_chunks filename num_chunks =
    let channel = In_channel.create filename in
    let total_bytes = In_channel.length channel in
    let chunk_size = Int64.( / ) total_bytes num_chunks in
    let start = 0L in
    let stop = chunk_size in
    let chunks = get_file_chunks_inner channel start stop chunk_size total_bytes [||] in
    In_channel.close channel;
    chunks
  ;;
end
