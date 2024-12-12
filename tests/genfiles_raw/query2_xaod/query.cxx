#include <analysis/query.h>
#include "xAODRootAccess/tools/TFileAccessTracer.h"


#include "xAODJet/JetContainer.h"


#include <TTree.h>

query :: query (const std::string& name,
                                  ISvcLocator *pSvcLocator)
    : EL::AnaAlgorithm (name, pSvcLocator)
  
{
  // Here you put any code for the base initialization of variables,
  // e.g. initialize all pointers to 0.  This is also where you
  // declare all properties for your algorithm.  Note that things like
  // resetting statistics variables or booking histograms should
  // rather go into the initialize() function.

  // Turn off file access statistics reporting. This is, according to Attila, useful
  // for GRID jobs, but not so much for other jobs. For those of us not located at CERN
  // and for a large amount of data, this can sometimes take a minute.
  // So we get rid of it.
  xAOD::TFileAccessTracer::enableDataSubmission(false);

  

}

StatusCode query :: initialize ()
{
  // Here you do everything that needs to be done at the very
  // beginning on each worker node, e.g. create histograms and output
  // trees.  This method gets called before any input files are
  // connected.

  
  {
  
    ANA_CHECK (book (TTree ("atlas_xaod_tree", "My analysis ntuple")));
  
    auto myTree = tree ("atlas_xaod_tree");
  
    myTree->Branch("pt", &_pt3);
  
    myTree->Branch("eta", &_eta4);
  
  }
  

  

  return StatusCode::SUCCESS;
}

StatusCode query :: execute ()
{
  // Here you do everything that needs to be done on every single
  // events, e.g. read input variables, apply cuts, and fill
  // histograms and trees.  This is where most of your actual analysis
  // code will go.

  
  {
  
    const xAOD::JetContainer* jets0;
  
    {
  
      const xAOD::JetContainer* result = 0;
  
      ANA_CHECK (evtStore()->retrieve(result, "AnalysisJets"));
  
      jets0 = result;
  
    }
  
    for (auto &&i_obj1 : *jets0)
  
    {
  
      _pt3.push_back(i_obj1->pt());
  
    }
  
    for (auto &&i_obj2 : *jets0)
  
    {
  
      _eta4.push_back(i_obj2->eta());
  
    }
  
    tree("atlas_xaod_tree")->Fill();
  
    _pt3.clear();
  
    _eta4.clear();
  
  }
  

  return StatusCode::SUCCESS;
}



StatusCode query :: finalize ()
{
  // This method is the mirror image of initialize(), meaning it gets
  // called after the last event has been processed on the worker node
  // and allows you to finish up any objects you created in
  // initialize() before they are written to disk.  This is actually
  // fairly rare, since this happens separately for each worker node.
  // Most of the time you want to do your post-processing on the
  // submission node after all your histogram outputs have been
  // merged.
  return StatusCode::SUCCESS;
}