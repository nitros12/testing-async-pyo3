use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use std::{sync::atomic::{Ordering::Relaxed, AtomicBool}, thread, time::Duration};

#[pyfunction]
fn rust_side_worker() {
    static STARTED: AtomicBool = AtomicBool::new(false);

    if STARTED.compare_and_swap(false, true, Relaxed) {
        return;
    }

    let num_threads = num_cpus::get().max(1);

    for _ in 0..num_threads {
        thread::spawn(|| smol::run(futures::future::pending::<()>()));
    }
}

#[pyfunction]
fn delay_test(delay: u64, result: PyObject) -> PyResult<PyObject> {
    let (fut, res_fut): (PyObject, PyObject) = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let asyncio = PyModule::import(py, "asyncio")?;
        let fut: PyObject = asyncio.call0("get_running_loop")?.call_method0("create_future")?.into();
        (fut.clone_ref(py), fut)
    };

    smol::Task::spawn(async move {
        smol::Timer::after(Duration::from_secs(delay)).await;

        {
            println!("setting result");
            let gil = Python::acquire_gil();
            let py = gil.python();
            if let Err(e) = fut.call_method1(py, "set_result", (result,)) {
                eprintln!("error occured: {:?}", e);
            }
            println!("done");
        }
    }).detach();

    Ok(res_fut)
}


#[pymodule]
fn async_py_rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(rust_side_worker))?;
    m.add_wrapped(wrap_pyfunction!(delay_test))?;

    Ok(())
}
